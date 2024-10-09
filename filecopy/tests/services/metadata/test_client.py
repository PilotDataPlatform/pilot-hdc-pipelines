# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from operations.minio_boto3_client import MinioBoto3Client
from operations.models import ItemStatus
from operations.models import NodeToRegister
from operations.models import ResourceType
from operations.models import get_timestamp


class TestMetadataServiceClient:
    def test_get_items_by_ids_returns_correct_nodes(self, metadata_service_client, httpserver, create_node, fake):
        _id = fake.pyint()
        node = create_node(id_=_id)
        expected_body = {'result': [node]}
        httpserver.expect_request('/v1/items/batch/').respond_with_json(expected_body)
        received_response = metadata_service_client.get_items_by_ids([_id])
        assert received_response[_id] == node

    def test_get_item_by_id_returns_correct_nodes(self, metadata_service_client, httpserver, create_node, fake):
        _id = fake.pyint()
        node = create_node(id_=_id)
        expected_body = {'result': [node]}
        httpserver.expect_request('/v1/items/batch/').respond_with_json(expected_body)
        received_response = metadata_service_client.get_item_by_id(_id)
        assert received_response == node

    def test_get_nodes_tree_returns_correct_nodes(self, metadata_service_client, httpserver, create_node, fake):
        _id = fake.pyint()
        start_node = create_node(id_=_id, name='test')
        node = create_node(parent_path='test')
        expected_query_body = {'result': start_node}
        expected_body = {'result': [node]}
        httpserver.expect_request(f'/v1/item/{_id}/').respond_with_json(expected_query_body)
        httpserver.expect_request('/v1/items/search/').respond_with_json(expected_body)
        received_response = metadata_service_client.get_nodes_tree(start_node.id)
        assert received_response[0] == node

    def test_update_node_returns_correct_nodes(self, metadata_service_client, httpserver, create_node, fake):
        _id = fake.pyint()
        node = create_node(id_=_id)
        updated_node = create_node(id_=_id, size=15)
        expected_body = {'result': updated_node}
        httpserver.expect_request('/v1/item/').respond_with_json(expected_body)
        update_json = {'size': 15}
        received_response = metadata_service_client.update_node(node, update_json)
        assert received_response['result'] == updated_node

    def test_update_copied_file_node_successed(self, metadata_service_client, httpserver, create_node, mocker):
        node = create_node(parent='admin', name='test', type_=ResourceType.FILE)
        source_file = create_node(
            location_uri='http://minio_path/core-testproject/admin/test', size=5, type_=ResourceType.FILE
        )
        new_node = create_node(parent='admin', name='test', version='test_versionId')
        expected_body = {'result': new_node}

        minio_client = MinioBoto3Client

        async def mock_copy_object(arg1, arg2, arg3, arg4):
            return {'VersionId': 'test_versionId'}

        mocker.patch('operations.services.metadata.client.MinioBoto3Client.copy_object', mock_copy_object)
        httpserver.expect_request('/v1/item/').respond_with_json(expected_body)
        received_response, versionId = metadata_service_client.update_copied_file_node(
            'testproject', node, ['system_tag'], source_file, minio_client
        )
        assert received_response == new_node
        assert versionId == 'test_versionId'

    def test_register_node_get_correct_nodes(self, metadata_service_client, httpserver, create_node):
        source_node = create_node(parent='admin', name='test')
        parent_node = create_node(name='test')
        node = create_node(container_code='testproject')
        expected_body = {'result': node}
        httpserver.expect_request('/v1/item/').respond_with_json(expected_body)
        timestamp = get_timestamp()
        received_response = metadata_service_client.register_node(
            'testproject', source_node, parent_node, 'file', ItemStatus.REGISTERED, timestamp
        )
        assert received_response == node

    def test_archived_node_correctly(self, metadata_service_client, httpserver, create_node, mocker):
        source_node = create_node(parent='admin', name='test')
        trash_node = create_node(
            parent='admin',
            name='test',
            type_=ResourceType.FILE,
            status=ItemStatus.ARCHIVED,
            location_uri='http://minio_path/core-testproject/admin/test',
        )
        expected_body = {'result': [trash_node]}
        httpserver.expect_request('/v1/item/').respond_with_json(expected_body)

        minio_client = MinioBoto3Client

        async def mock_remove_object(arg1, arg2):
            return None

        mocker.patch('operations.services.metadata.client.MinioBoto3Client.remove_object', mock_remove_object)

        async def mock_create_file_operation_logs(arg1, arg2, arg3, arg4):
            return None

        mocker.patch(
            'operations.services.metadata.client.KafkaProducer.create_file_operation_logs',
            mock_create_file_operation_logs,
        )
        received_response = metadata_service_client.archived_node(source_node, minio_client, 'delete', 'test')
        assert received_response == [trash_node]

    def test_remove_registrated_nodes_not_raise_error(self, metadata_service_client, httpserver, create_node):
        node = create_node(parent='admin', name='test', status=ItemStatus.REGISTERED)
        registered_file_nodes = {node.id: node}
        expected_body = {'result': [node]}
        httpserver.expect_request('/v1/item/').respond_with_json(expected_body)

        received_response = metadata_service_client.remove_registrated_nodes(registered_file_nodes)
        assert received_response is None

    def test_register_nodes_succeed(self, metadata_service_client, httpserver, create_node, mocker):
        node = create_node(container_code='testproject', type_=ResourceType.FILE)
        source_node = create_node(parent='admin', name='test')
        parent_node = create_node(name='test')
        register_file_nodes = [NodeToRegister(source_node, parent_node)]
        expected_body = {'result': node}
        httpserver.expect_request('/v1/item/').respond_with_json(expected_body)
        timestamp = get_timestamp()
        received_response = metadata_service_client.register_nodes(register_file_nodes, 'testproject', timestamp)
        assert received_response == {source_node.id: node}
