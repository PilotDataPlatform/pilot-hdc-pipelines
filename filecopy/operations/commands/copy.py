# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import atexit
import json
from pathlib import Path

import click
from common import ProjectClient
from operations.config import get_settings
from operations.kafka_producer import KafkaProducer
from operations.logger import logger
from operations.managers import CopyManager
from operations.managers import CopyPreparationManager
from operations.minio_boto3_client import MinioBoto3Client
from operations.models import get_timestamp
from operations.services.approval.client import ApprovalServiceClient
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
@click.option('--destination-id', type=str, required=True)
@click.option('--include-ids', type=str, multiple=True)
@click.option('--job-id', type=str, required=True)
@click.option('--session-id', type=str, required=True)
@click.option('--project-code', type=str, required=True)
@click.option('--operator', type=str, required=True)
@click.option('--request-info', type=str)
@click.option('--access-token', type=str, required=True)
def copy(
    source_id: str,
    destination_id: str,
    include_ids: tuple[str],
    job_id: str,
    session_id: str,
    project_code: str,
    operator: str,
    request_info: str | None,
    access_token: str,
):
    """Copy files from source geid into destination geid."""

    click.echo(f'Starting copy process from "{source_id}" into "{destination_id}" including only "{set(include_ids)}".')

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

    approval_service_client = None
    approved_entities = None

    # TODO: This sprint assume metadata service can always get info,
    #  Next sprint edit notification service client - https://indocconsortium.atlassian.net/browse/PILOT-1830
    nodes = metadata_service_client.get_items_by_ids([source_id, destination_id])
    include_nodes = metadata_service_client.get_items_by_ids(include_ids[0].split(','))
    include_node_ids = list(include_nodes.keys())
    source_folder = nodes[source_id]
    destination_folder = nodes[destination_id]
    target_names = [f'{item["parent_path"]}/{item["name"]}' for item in include_nodes.values()]
    target_type = 'batch' if len(target_names) > 1 else next(iter(include_nodes.values()))['type']
    notification_client = NotificationServiceClient(
        settings.NOTIFICATION_SERVICE,
        include_nodes,
        source_folder,
        destination_folder,
        project_code,
        PipelineAction.COPY,
        PipelineStatus.SUCCESS,
        operator,
        NotificationType.PIPELINE,
    )

    try:
        logger.audit(
            'Attempting to copy items (recursively including child items).',
            project_code=project_code,
            operator=operator,
            node_ids=include_node_ids,
            source_id=source_folder.id,
            destination_id=destination_folder.id,
        )
        if request_info:
            request_dict = json.loads(request_info)
            request_id = list(request_dict.keys())[0]
            approved_entities = request_dict[request_id]
            approval_service_client = ApprovalServiceClient(settings.APPROVAL_SERVICE, request_id)

        if destination_folder.is_archived:
            raise ValueError('Destination is already in trash bin')

        source_bucket = Path(f'gr-{project_code}')
        destination_bucket = Path(f'core-{project_code}')
        timestamp = get_timestamp()

        copy_preparation_manager = CopyPreparationManager(
            metadata_service_client,
            approval_service_client,
            approved_entities,
            project_code,
            settings.GREEN_ZONE_LABEL,
            settings.CORE_ZONE_LABEL,
            source_bucket,
            destination_bucket,
            set(include_ids[0].split(',')),
        )

        traverser = Traverser(copy_preparation_manager)
        traverser.traverse_tree(source_folder, destination_folder)
        registered_file_nodes = {}

        loop = asyncio.get_event_loop()
        project = loop.run_until_complete(metadata_service_client.get_project_by_code(project_code))

        try:
            dataops_client.lock_resources(copy_preparation_manager.read_lock_paths, ResourceLockOperation.READ)
            register_file_nodes = copy_preparation_manager.register_file_nodes
            registered_file_nodes = metadata_service_client.register_nodes(register_file_nodes, project_code, timestamp)
            source_file_node = copy_preparation_manager.source_file_node
            source_folder_nodes = copy_preparation_manager.source_folder_nodes

            operation_type = 'copy'

            system_tags = [settings.COPIED_WITH_APPROVAL_TAG]
            copy_manager = CopyManager(
                metadata_service_client,
                dataops_client,
                approval_service_client,
                approved_entities,
                system_tags,
                project,
                operator,
                minio_client,
                operation_type,
            )
            copy_manager.process_files(registered_file_nodes, source_file_node)
            copy_manager.process_folders(source_folder_nodes)
        finally:
            dataops_client.unlock_resources(copy_preparation_manager.read_lock_paths, ResourceLockOperation.READ)
            metadata_service_client.remove_registrated_nodes(registered_file_nodes)

        dataops_client.update_job(
            session_id=session_id,
            job_id=job_id,
            target_names=target_names,
            target_type=target_type,
            container_code=project_code,
            action_type='data_transfer',
            status=JobStatus.SUCCEED,
        )
        notification_client.send_notifications()
        click.echo('Copy operation has been finished successfully.')
        logger.audit(
            'Successfully managed to copy items (recursively including child items).',
            project_code=project_code,
            operator=operator,
            node_ids=include_node_ids,
            source_id=source_folder.id,
            destination_id=destination_folder.id,
        )
    except Exception as e:
        logger.audit(
            'Received an unexpected error while attempting to copy items (recursively including child items).',
            project_code=project_code,
            operator=operator,
            node_ids=include_node_ids,
            source_id=source_folder.id,
            destination_id=destination_folder.id,
        )
        click.echo(f'Exception occurred while performing copy operation:{e}')
        try:
            notification_client.set_status(PipelineStatus.FAILURE)
            notification_client.send_notifications()
            dataops_client.update_job(
                session_id=session_id,
                job_id=job_id,
                target_names=target_names,
                target_type=target_type,
                container_code=project_code,
                action_type='data_transfer',
                status=JobStatus.FAILED,
            )
        except Exception as e:
            click.echo(f'Update job error: {e}')
        exit(1)
