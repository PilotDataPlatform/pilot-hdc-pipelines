# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import time
from typing import Any

from common import ProjectClient
from common import ProjectException
from operations.kafka_producer import KafkaProducer
from operations.logger import logger
from operations.minio_boto3_client import MinioBoto3Client
from operations.models import ItemStatus
from operations.models import Node
from operations.models import NodeList
from operations.models import NodeToRegister
from operations.models import ResourceType
from operations.models import ZoneType
from operations.models import append_suffix_to_filepath
from requests import Session


class MetadataServiceClient:
    def __init__(
        self,
        endpoint: str,
        minio_endpoint: str,
        core_zone_label: str,
        temp_dir: str,
        project_client: ProjectClient,
        access_token: str,
    ) -> None:
        self.endpoint_v1 = f'{endpoint}/v1/'
        self.client = Session()

        self.minio_endpoint = minio_endpoint
        self.core_zone_label = core_zone_label
        self.temp_dir = temp_dir
        self.project_client = project_client
        self.access_token = access_token

    def get_item_by_id(self, node_id: str) -> Node:
        nodes = self.get_items_by_ids([node_id])
        return nodes[node_id]

    def get_items_by_ids(self, ids: list) -> dict[str, Node]:
        parameter = {'ids': ids}
        response = self.client.get(f'{self.endpoint_v1}items/batch/', params=parameter)
        if response.status_code != 200:
            raise Exception(f'Unable to get nodes by ids "{ids}".')

        results = response.json()['result']

        if len(results) != len(ids):
            raise Exception(f'Number of returned nodes does not match number of requested ids "{ids}".')

        nodes = {node.id: node for node in NodeList(results)}

        return nodes

    async def get_project_by_code(self, project_code: str) -> Node:
        try:
            project = await self.project_client.get(code=project_code)
            result = await project.json()
        except ProjectException:
            raise ProjectException(f'Unable to get project by code "{project_code}".')

        return Node(result)

    def get_nodes_tree(self, start_folder_id: str, traverse_subtrees: bool = False) -> NodeList:
        parent_folder_response = self.client.get(f'{self.endpoint_v1}item/{start_folder_id}/')
        if parent_folder_response.status_code != 200:
            raise Exception(
                f'Unable to get parent folder starting from "{start_folder_id}", "{parent_folder_response.url}".'
            )
        parent_folder = parent_folder_response.json()['result']

        header = {'Authorization': f'Bearer {self.access_token}'}

        parameters = {
            'status': ItemStatus.ACTIVE,
            'zone': parent_folder['zone'],
            'container_code': parent_folder['container_code'],
            'parent_path': self.format_folder_path(parent_folder, '/'),
            'recursive': traverse_subtrees,
            'page_size': 1000,
        }
        node_query_url = self.endpoint_v1 + 'items/search/'
        response = self.client.get(node_query_url, params=parameters, headers=header)

        if response.status_code != 200:
            raise Exception(f'Unable to get nodes tree starting from "{start_folder_id}".')

        nodes = NodeList(response.json()['result'])
        return nodes

    def update_node(self, node: Node, update_json: dict[str, Any]) -> dict[str, Any]:
        response = self.client.put(url=f'{self.endpoint_v1}item/', params={'id': node.get('id')}, json=update_json)

        if response.status_code != 200:
            raise Exception(f'Unable to update node with node id "{node["id"]}".')

        return response.json()

    def update_copied_file_node(
        self, project: str, node: Node, system_tags: list[str], source_file: Node, minio_client: MinioBoto3Client
    ) -> tuple[dict[str, Any], str]:
        try:
            file_display_path = self.format_folder_path(node, '/')
            location = f'minio://{self.minio_endpoint}/core-{project}/{file_display_path}'
            payload = {
                'status': ItemStatus.ACTIVE,
                'location_uri': location,
                'system_tags': system_tags,
            }
            # minio location is
            # minio://http://<end_point>/bucket/user/object_path
            src_minio_path = source_file['storage'].get('location_uri').split('//')[-1]
            _, src_bucket, src_obj_path = tuple(src_minio_path.split('/', 2))
            target_minio_path = location.split('//')[-1]
            _, target_bucket, target_obj_path = tuple(target_minio_path.split('/', 2))

            version_id = self._copy_file_node(
                node, src_bucket, src_obj_path, target_bucket, target_obj_path, minio_client
            )
            payload['version'] = version_id

            response = self.client.put(url=f'{self.endpoint_v1}item/', params={'id': node.get('id')}, json=payload)

            if response.status_code != 200:
                raise Exception(f'Unable to update node with node id "{node["id"]}".')
        except Exception:
            logger.exception('Error when copying.')
            raise
        return Node(response.json()['result']), version_id

    def _copy_file_node(
        self,
        node: Node,
        src_bucket: str,
        src_obj_path: str,
        target_bucket: str,
        target_obj_path: str,
        minio_client: MinioBoto3Client,
    ) -> str:
        """The minio api only accept the 5GB in copy.

        if >5GB we need to download to local then reupload to target
        """
        file_size_gb = node.size
        loop = asyncio.get_event_loop()
        if file_size_gb < 5e9:
            logger.info('File size less than 5GiB')
            logger.info(f'Copying object from "{src_bucket}/{src_obj_path}" to "{target_bucket}/{target_obj_path}".')
            result = loop.run_until_complete(
                minio_client.copy_object(target_bucket, target_obj_path, src_bucket, src_obj_path)
            )
            version_id = result.get('VersionId', '')  # empty in case versioning is unsupported
        else:
            logger.info('File size greater than 5GiB')
            temp_path = self.temp_dir + str(time.time())[0:10]
            temp_file_path = f'{temp_path}/{node.name}'
            loop.run_until_complete(minio_client.download_object(src_bucket, src_obj_path, temp_file_path))
            logger.info(f'File fetched to local disk: {temp_path}')
            result = loop.run_until_complete(
                minio_client.upload_object(target_bucket, target_obj_path, temp_file_path, temp_path)
            )
            version_id = result.get('VersionId', '')  # empty in case versioning is unsupported

        logger.info(f'Minio Copy {src_bucket}/{src_obj_path} Success')
        return version_id

    def register_node(
        self,
        project: str,
        source_node: Node,
        parent_node: Node,
        item_type: ResourceType,
        status: ItemStatus,
        timestamp: int | None,
        zone: ZoneType = ZoneType.CORE,
    ) -> Node:

        payload = {
            'parent': parent_node.id,
            'parent_path': self.format_folder_path(parent_node, '/'),
            'type': item_type,
            'zone': zone,
            'name': source_node.name,
            'size': source_node.size,
            'owner': source_node.owner,
            'container_code': project,
            'container_type': 'project',
            'tags': source_node.tags,
            'status': status,
        }
        # adding the attribute set if exist
        manifest = source_node.get_attributes()
        if manifest:
            payload['attribute_template_id'] = list(manifest.keys())[0]
            payload['attributes'] = manifest[list(manifest.keys())[0]]

        create_node_url = self.endpoint_v1 + 'item/'
        response = self.client.post(create_node_url, json=payload)
        if response.status_code == 409 and item_type == ResourceType.FILE:
            payload['name'] = append_suffix_to_filepath(source_node.name, timestamp)
            response = self.client.post(create_node_url, json=payload)
        elif response.status_code == 409 and item_type == ResourceType.FOLDER:
            folder_node = self.get_node_by_full_path(
                source_node.name, self.format_folder_path(parent_node, '/'), project
            )
            return Node(folder_node)
        response.raise_for_status()
        new_node = response.json()['result']
        return Node(new_node)

    def register_file(
        self,
        project: str,
        source_node: Node,
        parent_node: Node,
        zone: ZoneType = ZoneType.CORE,
    ) -> Node:
        return self.register_node(
            project, source_node, parent_node, ResourceType.FILE, ItemStatus.REGISTERED, None, zone
        )

    def register_folder(
        self,
        project: str,
        source_node: Node,
        parent_node: Node,
        zone: ZoneType = ZoneType.CORE,
    ) -> Node:
        return self.register_node(project, source_node, parent_node, ResourceType.FOLDER, ItemStatus.ACTIVE, None, zone)

    def get_name_folder(self, username: str, project_code: str, zone: ZoneType = ZoneType.GREENROOM) -> Node:
        params = {
            'name': username,
            'container_code': project_code,
            'container_type': 'project',
            'zone': zone,
            'status': ItemStatus.ACTIVE,
        }
        response = self.client.get(f'{self.endpoint_v1}item/', params=params)
        if response.status_code != 200:
            raise Exception(f'Folder {project_code}/{zone.name}/{username} does not exist')
        return Node(response.json()['result'])

    def get_node_by_full_path(self, name: str, parent_path: str, container_code: str) -> dict[str, Any]:
        param = {
            'name': name,
            'parent_path': parent_path,
            'container_code': container_code,
            'container_type': 'project',
            'zone': ZoneType.CORE,
            'status': ItemStatus.ACTIVE,
        }
        get_node_url = self.endpoint_v1 + 'item/'
        response = self.client.get(get_node_url, params=param)
        if response.status_code != 200:
            raise Exception(f'Item {parent_path}/{name} does not exist')
        return response.json()['result']

    def format_folder_path(self, node: Node, divider: str) -> str:
        parent_path = node.get('parent_path', None)
        if parent_path:
            return f'{parent_path}{divider}{node.get("name")}'
        return node.get('name')

    def move_node_to_trash(self, node_id: str) -> list:
        patch_node_url = self.endpoint_v1 + 'item/'
        parameter = {'id': node_id, 'status': ItemStatus.ARCHIVED}
        response = self.client.patch(patch_node_url, params=parameter)
        if response.status_code != 200:
            raise Exception(f'Unable to patch node with node id "{node_id}".')
        trash_node = response.json()['result']
        return trash_node

    def archived_node(self, source_file: Node, minio_client: MinioBoto3Client, operation_type, operator) -> Node:
        trash_node = self.move_node_to_trash(source_file.id)
        try:
            # minio location is
            # minio://http://<end_point>/bucket/user/object_path
            for item in trash_node:
                if item['type'] == ResourceType.FILE:
                    # # Remove from minio
                    # src_minio_path = item['storage'].get('location_uri').split('//')[-1]
                    # _, src_bucket, src_obj_path = tuple(src_minio_path.split('/', 2))
                    #
                    loop = asyncio.get_event_loop()
                    # loop.run_until_complete(minio_client.remove_object(src_bucket, src_obj_path))
                    # logger.info(f'Minio Delete {src_bucket}/{src_obj_path} Success')

                    # Add activity log
                    loop.run_until_complete(
                        KafkaProducer.create_file_operation_logs(Node(item), operation_type, operator, None)
                    )

        except Exception as e:
            logger.exception(f'Error when removing file: {e}')
            raise

        return trash_node

    def remove_registrated_nodes(self, registered_file_nodes: dict[str, Node]) -> None:
        """Remove the registrated file nodes when copy failed for tear down."""
        delete_node_url = self.endpoint_v1 + 'item/'
        for item in registered_file_nodes:
            node = registered_file_nodes[item]
            if node.status == ItemStatus.REGISTERED:
                param = {'id': node.id}
                response = self.client.delete(delete_node_url, params=param)
                if response.status_code != 200:
                    raise Exception(f'Unable to patch node with node id "{node.id}".')

    def register_nodes(
        self, register_file_nodes: list[NodeToRegister], project_code: str, timestamp: int
    ) -> dict[str, Node]:
        """Registered the file nodes."""
        try:
            registered_file_nodes = {}
            for item in register_file_nodes:
                node = self.register_node(
                    project_code,
                    item.source_node,
                    item.destination_node,
                    ResourceType.FILE,
                    ItemStatus.REGISTERED,
                    timestamp,
                )
                registered_file_nodes[item.source_node.id] = node
        except Exception as e:
            raise Exception(f'Unable to registered nodes: {e}')
        return registered_file_nodes
