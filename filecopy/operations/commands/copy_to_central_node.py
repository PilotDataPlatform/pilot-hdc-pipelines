# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import base64
import datetime as dt
from functools import wraps
from pathlib import Path
from uuid import UUID
from uuid import uuid4

import click
from common import ProjectClient
from common import get_boto3_client
from operations.config import get_settings
from operations.logger import logger
from operations.models import get_timestamp
from operations.services.central_node.client import CentralNodeClient
from operations.services.dataops.client import DataopsServiceClient
from operations.services.dataops.client import JobStatus
from operations.services.metadata.client import MetadataServiceClient


def click_command_async(f):
    @click.command()
    @wraps(f)
    def wrapper(*args, **kwds):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwds))

    return wrapper


@click_command_async
@click.option('--file-id', type=UUID, required=True)
@click.option('--destination-api-url-base64', type=str, required=True)
@click.option('--destination-project-code', type=str, required=True)
@click.option('--destination-access-token', type=str, required=True)
@click.option('--job-id', type=str, required=True)
@click.option('--session-id', type=str, required=True)
@click.option('--operator', type=str, required=True)
@click.option('--access-token', type=str, required=True)
async def copy_to_central_node(
    file_id: UUID,
    destination_api_url_base64: str,
    destination_project_code: str,
    destination_access_token: str,
    job_id: str,
    session_id: str,
    operator: str,
    access_token: str,
):
    """Copy to the central node."""

    click.echo(
        f'Starting copy to the central node process for file id "{file_id}" into '
        f'"{destination_project_code}" destination project.'
    )

    settings = get_settings()

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
    minio_client = await get_boto3_client(
        endpoint=settings.S3_URL,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        https=settings.S3_INTERNAL_HTTPS,
    )
    central_node_client = CentralNodeClient(
        endpoint=base64.urlsafe_b64decode(destination_api_url_base64.encode()).decode(),
        access_token=destination_access_token,
        session_id=session_id,
    )

    source_file = metadata_service_client.get_item_by_id(str(file_id))
    source_project_code = source_file.container_code
    copy_unique_id = dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%d') + '-' + str(get_timestamp())
    source_file_name, *extensions = source_file.name.rsplit('.', 1)
    destination_file_name = f'{source_file_name}-{copy_unique_id}'
    if extensions:
        destination_file_name += f'.{".".join(extensions)}'

    try:
        logger.audit(
            'Attempting to copy to the central node.',
            operator=operator,
            file_id=source_file.id,
            project_code=source_project_code,
        )

        temp_file_path = Path(f'{settings.TEMP_DIR}/{uuid4()}')
        source_file_location = source_file.file_bucket_location

        try:
            await minio_client.download_object(
                source_file_location.bucket_name, source_file_location.object_path, str(temp_file_path)
            )
            logger.info(f'File successfully downloaded into the temporary file "{temp_file_path}".')

            await central_node_client.upload_file_to_project(
                file_path=temp_file_path,
                destination_file_name=destination_file_name,
                project_code=destination_project_code,
                chunk_size=20 * 1024 * 1024,
                max_concurrent=4,
            )
        except Exception as e:
            logger.exception('Error occurred while copying to the central node.')
            raise e
        finally:
            temp_file_path.unlink(missing_ok=True)

        dataops_client.update_job(
            session_id=session_id,
            job_id=job_id,
            target_names=[source_file.name],
            target_type='file',
            container_code=source_project_code,
            action_type='data_import',
            status=JobStatus.SUCCEED,
        )
        click.echo('Copy to the central node operation has been finished successfully.')
        logger.audit(
            'Successfully managed to copy to the central node.',
            operator=operator,
            file_id=source_file.id,
            project_code=source_project_code,
        )
    except Exception as e:
        logger.audit(
            'Received an unexpected error while attempting to copy to the central node.',
            operator=operator,
            file_id=source_file.id,
            project_code=source_project_code,
        )
        click.echo(f'Exception occurred while performing copy to the central node operation: {e}')
        try:
            dataops_client.update_job(
                session_id=session_id,
                job_id=job_id,
                target_names=[source_file.name],
                target_type='file',
                container_code=source_project_code,
                action_type='data_import',
                status=JobStatus.FAILED,
            )
        except Exception as e:
            click.echo(f'Update job error: {e}')
        exit(1)
