# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import datetime as dt
import shutil
import zipfile
from pathlib import Path
from uuid import UUID

import click
from common import ProjectClient
from operations.config import get_settings
from operations.logger import logger
from operations.managers import ShareDatasetManager
from operations.minio_boto3_client import MinioBoto3Client
from operations.models import Node
from operations.models import ZoneType
from operations.models import get_timestamp
from operations.services.dataops.client import DataopsServiceClient
from operations.services.dataops.client import JobStatus
from operations.services.dataset.client import DatasetServiceClient
from operations.services.metadata.client import MetadataServiceClient
from operations.traverser import Traverser


class FileBucketLocation:
    def __init__(self, location: str) -> None:
        self.location = location
        self.file_full_path = location.split('//')[-1]
        _, self.bucket_name, self.object_path = tuple(self.file_full_path.split('/', 2))


@click.command()
@click.option('--version-id', type=UUID, required=True)
@click.option('--destination-project-code', type=str, required=True)
@click.option('--job-id', type=str, required=True)
@click.option('--session-id', type=str, required=True)
@click.option('--operator', type=str, required=True)
@click.option('--access-token', type=str, required=True)
def share_dataset_version(
    version_id: UUID,
    destination_project_code: str,
    job_id: str,
    session_id: str,
    operator: str,
    access_token: str,
):
    """Copy dataset version into project."""

    click.echo(f'Starting copy process for dataset version "{version_id}" into "{destination_project_code}" project.')

    settings = get_settings()
    loop = asyncio.get_event_loop()

    dataset_service_client = DatasetServiceClient(settings.DATASET_SERVICE)
    project_service_client = ProjectClient(settings.PROJECT_SERVICE, settings.REDIS_URL)
    metadata_service_client = MetadataServiceClient(
        settings.METADATA_SERVICE,
        settings.S3_URL,
        settings.CORE_ZONE_LABEL,
        settings.TEMP_DIR,
        project_service_client,
        access_token,
    )
    dataops_client = DataopsServiceClient(settings.DATAOPS_SERVICE)
    minio_client = MinioBoto3Client(
        settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY, settings.S3_URL, settings.S3_INTERNAL_HTTPS
    )

    dataset_version = dataset_service_client.get_dataset_version(version_id)
    destination_folder = metadata_service_client.get_name_folder(
        username=operator, project_code=destination_project_code, zone=ZoneType.GREENROOM
    )

    share_unique_id = dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%d') + '-' + str(get_timestamp())
    destination_folder_name = f'{dataset_version["dataset_code"]}-v{dataset_version["version"]}-{share_unique_id}'

    try:
        dataset_version_destination_folder = Node(
            {
                'name': destination_folder_name,
                'size': 0,
                'owner': operator,
                'extended': {'extra': {'tags': []}},
            }
        )

        destination_folder_node = metadata_service_client.register_folder(
            destination_project_code,
            dataset_version_destination_folder,
            destination_folder,
            ZoneType.GREENROOM,
        )

        temp_extract_to_path = Path(f'{settings.TEMP_DIR}/{destination_folder_name}')
        temp_file_path = Path(f'{temp_extract_to_path}.zip')

        try:
            version_file = FileBucketLocation(dataset_version['location'])
            loop.run_until_complete(
                minio_client.client.download_object(
                    version_file.bucket_name, version_file.object_path, str(temp_file_path)
                )
            )
            logger.info(f'Dataset version successfully downloaded into the temporary file "{temp_file_path}".')

            temp_extract_to_path.mkdir()
            with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_to_path)
            logger.info(f'Dataset version successfully extracted into the temporary folder "{temp_extract_to_path}".')

            share_dataset_manager = ShareDatasetManager(
                metadata_service_client, minio_client, destination_project_code, ZoneType.GREENROOM, operator
            )

            temp_extract_to_path_as_node = Node(
                {
                    'name': temp_extract_to_path.name,
                    'parent_path': temp_extract_to_path.parent,
                }
            )
            traverser = Traverser(share_dataset_manager)
            traverser.traverse_tree(temp_extract_to_path_as_node, destination_folder_node)
        except Exception as e:
            logger.exception('Error occurred while traversing dataset version tree.')
            raise e
        finally:
            temp_file_path.unlink(missing_ok=True)
            shutil.rmtree(temp_extract_to_path, ignore_errors=True)

        dataops_client.update_job(
            session_id=session_id,
            job_id=job_id,
            target_names=[destination_folder_name],
            target_type='file',
            container_code=destination_project_code,
            action_type='data_import',
            status=JobStatus.SUCCEED,
        )
        click.echo('Copy dataset version operation has been finished successfully.')
    except Exception as e:
        click.echo(f'Exception occurred while performing copy dataset version operation:{e}')
        try:
            dataops_client.update_job(
                session_id=session_id,
                job_id=job_id,
                target_names=[destination_folder_name],
                target_type='file',
                container_code=destination_project_code,
                action_type='data_import',
                status=JobStatus.FAILED,
            )
        except Exception as e:
            click.echo(f'Update job error: {e}')
        exit(1)
