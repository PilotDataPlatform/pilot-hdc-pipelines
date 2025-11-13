# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import atexit
from pathlib import Path

import click
from common import ProjectClient
from operations.config import get_settings
from operations.kafka_producer import KafkaProducer
from operations.logger import logger
from operations.managers import DeleteManager
from operations.managers import DeletePreparationManager
from operations.minio_boto3_client import MinioBoto3Client
from operations.models import ZoneType
from operations.services.dataops.client import DataopsServiceClient
from operations.services.dataops.client import JobStatus
from operations.services.dataops.client import ResourceLockOperation
from operations.services.metadata.client import MetadataServiceClient
from operations.services.notification.client import NotificationServiceClient
from operations.services.notification.models import NotificationType
from operations.services.notification.models import PipelineAction
from operations.services.notification.models import PipelineStatus
from operations.traverser import Traverser

atexit.register(KafkaProducer.close_connection)


@click.command()
@click.option('--source-id', type=str, required=True)
@click.option('--include-ids', type=str, multiple=True)
@click.option('--job-id', type=str, required=True)
@click.option('--session-id', type=str, required=True)
@click.option('--project-code', type=str, required=True)
@click.option('--operator', type=str, required=True)
@click.option('--access-token', type=str, required=True)
def delete(
    source_id: str,
    include_ids: tuple[str],
    job_id: str,
    session_id: str,
    project_code: str,
    operator: str,
    access_token: str,
):
    """Move files from source geid into trash bin."""

    click.echo(f'Starting delete process from "{source_id} including only "{set(include_ids)}".')

    settings = get_settings()

    project_client = ProjectClient(settings.PROJECT_SERVICE, settings.REDIS_URL)

    metadata_service_client = MetadataServiceClient(
        settings.METADATA_SERVICE,
        settings.S3_URL,
        settings.CORE_ZONE_LABEL,
        settings.TEMP_DIR,
        project_client,
        access_token,
    )
    dataops_client = DataopsServiceClient(settings.DATAOPS_SERVICE)

    minio_client = MinioBoto3Client(
        settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY, settings.S3_URL, settings.S3_INTERNAL_HTTPS
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(KafkaProducer.init_connection())

    # TODO: This sprint assume metadata service can always get info,
    #  Next sprint edit notification service client - https://indocconsortium.atlassian.net/browse/PILOT-1830
    source_folder = metadata_service_client.get_item_by_id(source_id)
    source_path = None
    source_zone = None
    if source_folder:
        source_path = str(source_folder.display_path)
        source_zone = (
            settings.GREEN_ZONE_LABEL if source_folder.zone == ZoneType.GREENROOM else settings.CORE_ZONE_LABEL
        )
    destination_folder = Path()
    include_nodes = metadata_service_client.get_items_by_ids(include_ids[0].split(','))
    include_node_ids = list(include_nodes.keys())
    target_names = [f'{item["parent_path"]}/{item["name"]}' for item in include_nodes.values()]
    target_type = 'batch' if len(target_names) > 1 else next(iter(include_nodes.values()))['type']
    notification_client = NotificationServiceClient(
        settings.NOTIFICATION_SERVICE,
        include_nodes,
        source_folder,
        None,
        project_code,
        PipelineAction.DELETE,
        PipelineStatus.SUCCESS,
        operator,
        NotificationType.PIPELINE,
    )

    try:
        logger.audit(
            'Attempting to move items into trash bin.',
            project_code=project_code,
            operator=operator,
            node_ids=include_node_ids,
            source_id=source_id,
            source_path=source_path,
            source_zone=source_zone,
        )
        source_zone = settings.GREEN_ZONE_LABEL
        source_bucket = Path(f'gr-{project_code}')
        if source_folder['zone'] == ZoneType.CORE:
            source_zone = settings.CORE_ZONE_LABEL
            source_bucket = Path(f'core-{project_code}')

        delete_preparation_manager = DeletePreparationManager(
            metadata_service_client,
            project_code,
            source_zone,
            source_bucket,
            set(include_ids[0].split(',')),
        )
        traverser = Traverser(delete_preparation_manager)
        traverser.traverse_tree(source_folder, destination_folder)

        loop = asyncio.get_event_loop()
        project = loop.run_until_complete(metadata_service_client.get_project_by_code(project_code))

        try:
            dataops_client.lock_resources(delete_preparation_manager.write_lock_paths, ResourceLockOperation.WRITE)

            pipeline_name = 'data_delete_folder'
            pipeline_desc = 'the script will delete the folder in greenroom/core recursively'
            operation_type = 'delete'

            delete_manager = DeleteManager(
                metadata_service_client,
                dataops_client,
                project,
                operator,
                minio_client,
                settings.CORE_ZONE_LABEL,
                settings.GREEN_ZONE_LABEL,
                pipeline_name,
                pipeline_desc,
                operation_type,
                set(include_ids[0].split(',')),
            )

            delete_manager.archive_nodes()

        finally:
            dataops_client.unlock_resources(delete_preparation_manager.write_lock_paths, ResourceLockOperation.WRITE)

        dataops_client.update_job(
            session_id=session_id,
            job_id=job_id,
            target_names=target_names,
            target_type=target_type,
            container_code=project_code,
            action_type='data_delete',
            status=JobStatus.SUCCEED,
        )
        notification_client.send_notifications()
        click.echo('Delete operation has been finished successfully.')
        logger.audit(
            'Successfully managed to move items into trash bin.',
            project_code=project_code,
            operator=operator,
            node_ids=include_node_ids,
            source_id=source_id,
            source_path=source_path,
            source_zone=source_zone,
        )
    except Exception as e:
        logger.audit(
            'Received an unexpected error while attempting to move items into trash bin.',
            project_code=project_code,
            operator=operator,
            node_ids=include_node_ids,
            source_id=source_id,
            source_path=source_path,
            source_zone=source_zone,
        )
        click.echo(f'An exception occurred while performing delete operation: {e}')
        try:
            notification_client.set_status(PipelineStatus.FAILURE)
            notification_client.send_notifications()
            dataops_client.update_job(
                session_id=session_id,
                job_id=job_id,
                target_names=target_names,
                target_type=target_type,
                container_code=project_code,
                action_type='data_delete',
                status=JobStatus.FAILED,
            )
        except Exception as e:
            click.echo(f'Update job error: {e}')
        exit(1)
