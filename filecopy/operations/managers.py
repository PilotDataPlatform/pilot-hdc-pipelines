# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import os
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

from operations.duplicated_file_names import DuplicatedFileNames
from operations.kafka_producer import KafkaProducer
from operations.logger import logger
from operations.minio_boto3_client import MinioBoto3Client
from operations.models import ItemStatus
from operations.models import Node
from operations.models import NodeList
from operations.models import NodeToRegister
from operations.models import ResourceType
from operations.models import get_timestamp
from operations.services.approval.client import ApprovalServiceClient
from operations.services.dataops.client import DataopsServiceClient
from operations.services.metadata.client import MetadataServiceClient
from operations.services.redis.client import RedisClient


class NodeManager:
    """Base class for node manipulations that is used in the Traverser."""

    def __init__(self, metadata_service_client: MetadataServiceClient) -> None:
        self.metadata_service_client = metadata_service_client

    def get_tree(self, source_folder: Node) -> NodeList:
        """Return child nodes from current source folder."""
        nodes = self.metadata_service_client.get_nodes_tree(source_folder.id, False)
        return nodes

    def exclude_nodes(self, nodes: NodeList) -> Set[str]:
        """Return set of ids that should be excluded when copying from this source folder."""

        return set()

    def process_file(self, source_file: Node, destination_folder: Union[Path, Node]) -> None:
        """Process one file."""

        raise NotImplementedError

    def process_folder(self, source_folder: Node, destination_parent_folder: Union[Path, Node]) -> Union[Path, Node]:
        """Process one folder."""

        raise NotImplementedError


class BaseCopyManager(NodeManager):
    """Base manager for copying process with approved entities."""

    def __init__(
        self,
        metadata_service_client: MetadataServiceClient,
        approval_service_client: Optional[ApprovalServiceClient],
        approved_entities: Optional[List[str]],
        include_ids: Optional[Set[str]],
    ) -> None:
        super().__init__(metadata_service_client)

        self.approval_service_client = approval_service_client
        self.approved_entities = approved_entities
        self.include_ids = include_ids

    def _is_node_approved(self, node: Node) -> bool:
        """Check if node geid is in a list of approved entities.

        If approved entities are not set then node is considered approved.
        """

        if self.approved_entities is None:
            return True

        return node.id in self.approved_entities

    def exclude_nodes(self, nodes: NodeList) -> Set[str]:
        """Return set of geids that should be excluded when copying from this source folder."""
        if self.approved_entities is not None:
            excluded_geids = nodes.ids.difference(self.approved_entities)
            return excluded_geids

        if self.include_ids is None:
            return set()

        if not self.include_ids.issubset(nodes.ids):
            return set()
        excluded_geids = nodes.ids.difference(self.include_ids)

        return excluded_geids

    def process_file(self, source_file: Node, destination_folder: Union[Path, Node]) -> None:
        """Process one file."""

        raise NotImplementedError

    def process_folder(self, source_folder: Node, destination_parent_folder: Union[Path, Node]) -> Union[Path, Node]:
        """Process one folder."""

        raise NotImplementedError


class CopyManager:
    """Manager to copying files."""

    def __init__(
        self,
        metadata_service_client: MetadataServiceClient,
        dataops_client: DataopsServiceClient,
        approval_service_client: Optional[ApprovalServiceClient],
        approved_entities: List[str],
        system_tags: List[str],
        project: Node,
        operator: str,
        minio_client: MinioBoto3Client,
        operation_type: str,
    ) -> None:

        self.metadata_service_client = metadata_service_client
        self.approval_service_client = approval_service_client
        self.approved_entities = approved_entities

        self.dataops_client = dataops_client
        self.system_tags = system_tags
        self.project_code = project['code']
        self.operator = operator
        self.minio_client = minio_client
        self.operation_type = operation_type

    def _create_file_metadata(self, source_node: Node, target_node: Node, new_node_version_id: str) -> None:
        self._copy_zip_preview_info(source_node.id, target_node.id)

        update_json = {
            'system_tags': self.system_tags,
            'version': new_node_version_id,
        }
        self.metadata_service_client.update_node(source_node, update_json)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            KafkaProducer.create_file_operation_logs(source_node, self.operation_type, self.operator, target_node)
        )

    def _copy_zip_preview_info(self, old_geid, new_geid) -> None:
        """Transfer the saved preview info to copied one."""

        response = self.dataops_client.get_zip_preview(old_geid)
        if response is None:
            return

        self.dataops_client.create_zip_preview(new_geid, response['archive_preview'])

    def _update_approval_entity_copy_status_for_node(self, node: Node) -> None:
        """Update copy status field for approval entity related to node."""
        if not self.approval_service_client:
            return

        if not self.approved_entities:
            return

        self.approval_service_client.update_copy_status(node.id)

    def _process_file(self, source_file: Node, destination_file: Node) -> Node:

        logger.info(f'Processing source file "{source_file}" against destination file "{destination_file}".')

        # TODO: Need to refactory the minio copy here:
        # https://indocconsortium.atlassian.net/browse/PILOT-2501
        node, version_id = self.metadata_service_client.update_copied_file_node(
            self.project_code,
            destination_file,
            self.system_tags,
            source_file,
            self.minio_client,
        )

        self._create_file_metadata(source_file, node, version_id)

        self._update_approval_entity_copy_status_for_node(source_file)
        return node

    def process_files(self, registered_file_nodes: Dict[str, Node], source_file_node: Dict[str, Node]) -> None:
        for item in registered_file_nodes:
            updated_node = self._process_file(source_file_node[item], registered_file_nodes[item])
            registered_file_nodes[item] = updated_node

    def process_folders(self, source_folders: Dict[str, Node]) -> None:
        update_json = {'system_tags': self.system_tags}
        for _, item in source_folders.items():
            self.metadata_service_client.update_node(item, update_json)


class CopyPreparationManager(BaseCopyManager):
    """Manager to prepare data before start of copying process."""

    def __init__(
        self,
        metadata_service_client: MetadataServiceClient,
        approval_service_client: Optional[ApprovalServiceClient],
        approved_entities: List[str],
        project_code: str,
        source_zone: str,
        destination_zone: str,
        source_bucket: Path,
        destination_bucket: Path,
        include_geids: Optional[Set[str]],
    ) -> None:
        super().__init__(metadata_service_client, approval_service_client, approved_entities, include_geids)

        self.project_code = project_code
        self.source_zone = source_zone
        self.destination_zone = destination_zone
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.register_file_nodes = []
        self.source_file_node = {}
        self.source_folder_nodes = {}
        self.read_lock_paths = []

    def process_file(self, source_file: Node, destination_folder: Node) -> None:
        if not self._is_node_approved(source_file):
            return

        logger.info(f'Processing source file "{source_file}" against destination path "{destination_folder}".')
        source_filepath = self.source_bucket / source_file.display_path

        self.read_lock_paths.append(source_filepath)

        self.register_file_nodes.append(NodeToRegister(source_file, destination_folder))
        self.source_file_node[source_file.id] = source_file

    def process_folder(self, source_folder: Node, destination_parent: Node) -> Node:
        logger.info(
            f'Processing source folder "{source_folder}" ' f'against destination parent path "{destination_parent}".'
        )
        source_path = self.source_bucket / source_folder.display_path
        node = self.metadata_service_client.register_node(
            self.project_code, source_folder, destination_parent, ResourceType.FOLDER, ItemStatus.ACTIVE, None
        )
        self.source_folder_nodes[source_folder.id] = source_folder
        self.read_lock_paths.append(source_path)
        return node


class DeleteManager(NodeManager):
    """Manager to deleting files."""

    def __init__(
        self,
        metadata_service_client: MetadataServiceClient,
        dataops_client: DataopsServiceClient,
        project: Node,
        operator: str,
        minio_client: MinioBoto3Client,
        core_zone_label: str,
        green_zone_label: str,
        pipeline_name: str,
        pipeline_desc: str,
        operation_type: str,
        include_geids: Optional[Set[str]],
    ) -> None:
        super().__init__(metadata_service_client)

        self.dataops_client = dataops_client

        self.removal_timestamp = get_timestamp()
        self.project_code = project['code']
        self.operator = operator
        self.minio_client = minio_client
        self.core_zone_label = core_zone_label
        self.green_zone_label = green_zone_label
        self.pipeline_name = pipeline_name
        self.pipeline_desc = pipeline_desc
        self.operation_type = operation_type
        self.include_geids = include_geids

        self.redis_client = RedisClient()

    def exclude_nodes(self, nodes: NodeList) -> Set[str]:
        if self.include_geids is None:
            return set()

        if not self.include_geids.issubset(nodes.ids):
            return set()

        excluded_geids = nodes.ids.difference(self.include_geids)

        return excluded_geids

    def process_file(self, source_file: Node, destination_folder: Node) -> None:
        logger.info(f'Processing source file "{source_file}" against destination folder "{destination_folder}".')

    def process_folder(self, source_folder: Node, destination_parent_folder: Node) -> Node:
        logger.info(
            f'Processing source folder "{source_folder}" '
            f'against destination parent folder "{destination_parent_folder}".'
        )

    def archive_nodes(self) -> None:
        for node_id in self.include_geids:
            logger.info(f'Move the node "{node_id}" into trashbin recursively')
            node = self.metadata_service_client.get_item_by_id(node_id)
            self.metadata_service_client.archived_node(node, self.minio_client, self.operation_type, self.operator)

            # also remove the it from upload cache if it exists
            # so the key will be <zone>/<bucket>/<object_path>
            # please make sure it is same in upload service
            bucket_prefix = {1: 'core'}.get(node.zone, 'greenroom')
            obj_path = os.path.join(bucket_prefix, node.container_code, node.parent_path, node.name)

            if self.redis_client.check_by_key(obj_path):
                self.redis_client.delete_by_key(obj_path)


class DeletePreparationManager(NodeManager):
    """Manager to prepare data before start of deleting process."""

    def __init__(
        self,
        metadata_service_client: MetadataServiceClient,
        project_code: str,
        source_zone: str,
        source_bucket: Path,
        include_geids: Optional[Set[str]],
    ) -> None:
        super().__init__(metadata_service_client)

        self.project_code = project_code
        self.source_zone = source_zone
        self.source_bucket = source_bucket
        self.include_geids = include_geids

        self.duplicated_files = DuplicatedFileNames()
        self.write_lock_paths = []

    def exclude_nodes(self, nodes: NodeList) -> Set[str]:
        if self.include_geids is None:
            return set()

        if not self.include_geids.issubset(nodes.ids):
            return set()

        excluded_geids = nodes.ids.difference(self.include_geids)

        return excluded_geids

    def process_file(self, source_file: Node, destination_path: Path) -> None:
        logger.info(f'Processing source file "{source_file}" against destination path "{destination_path}".')

        source_filepath = self.source_bucket / source_file.display_path

        self.write_lock_paths.append(source_filepath)

    def process_folder(self, source_folder: Node, destination_parent_path: Path) -> Path:
        logger.info(
            f'Processing source folder "{source_folder}" '
            f'against destination parent path "{destination_parent_path}".'
        )

        source_path = self.source_bucket / source_folder.display_path

        self.write_lock_paths.append(source_path)

        return destination_parent_path
