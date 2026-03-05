# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from pathlib import Path
from uuid import UUID

import httpx
import pytest
from operations.services.central_node.client import CentralNodeClient
from pytest_httpserver import HTTPServer
from tests.fixtures.fake import Faker


class TestCentralNodeClient:

    async def test_get_name_folder_id_returns_uuid_of_matching_folder(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        folder_id = fake.uuid4()
        username = central_node_client.username
        expected_body = {
            'result': [
                {'id': fake.uuid4(), 'type': 'other_folder', 'name': username},
                {'id': folder_id, 'type': 'name_folder', 'name': username},
            ]
        }

        httpserver.expect_request('/pilot/portal/v1/files/meta').respond_with_json(expected_body)

        result = await central_node_client.get_name_folder_id(fake.project_code())
        assert result == UUID(folder_id)

    async def test_get_name_folder_id_raises_when_folder_not_found(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        httpserver.expect_request('/pilot/portal/v1/files/meta').respond_with_json({'result': []})

        with pytest.raises(ValueError, match='Name folder'):
            await central_node_client.get_name_folder_id(fake.project_code())

    async def test_get_name_folder_id_raises_on_http_error(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        httpserver.expect_request('/pilot/portal/v1/files/meta').respond_with_data(status=403)

        with pytest.raises(httpx.HTTPStatusError):
            await central_node_client.get_name_folder_id(fake.project_code())

    async def test_file_pre_upload_returns_first_result(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        project_code = fake.project_code()
        expected = {'job_id': fake.uuid4(), 'payload': {'resumable_identifier': fake.uuid4(), 'item_id': fake.uuid4()}}

        httpserver.expect_request(f'/pilot/portal/v1/project/{project_code}/files', method='POST').respond_with_json(
            {'result': [expected]}
        )

        result = await central_node_client.file_pre_upload(project_code, fake.file_name(), fake.uuid4())
        assert result == expected

    async def test_file_pre_upload_raises_on_http_error(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        project_code = fake.project_code()
        httpserver.expect_request(f'/pilot/portal/v1/project/{project_code}/files', method='POST').respond_with_data(
            status=500
        )

        with pytest.raises(httpx.HTTPStatusError):
            await central_node_client.file_pre_upload(project_code, fake.file_name(), fake.uuid4())

    async def test_file_post_upload_returns_result(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        file_id = fake.uuid4()
        expected = {'status': 'success', 'id': file_id}

        httpserver.expect_request('/pilot/upload/gr/v1/files', method='POST').respond_with_json({'result': expected})

        result = await central_node_client.file_post_upload(
            project_code=fake.project_code(),
            file_id=file_id,
            parent_folder_name=fake.user_name(),
            filename=fake.file_name(),
            total_chunks=3,
            total_bytes=1024,
            upload_id=fake.uuid4(),
            job_id=fake.uuid4(),
        )
        assert result == expected

    async def test_file_post_upload_raises_on_http_error(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        httpserver.expect_request('/pilot/upload/gr/v1/files', method='POST').respond_with_data(status=400)

        with pytest.raises(httpx.HTTPStatusError):
            await central_node_client.file_post_upload(
                fake.project_code(), fake.uuid4(), fake.user_name(), fake.file_name(), 1, 1, fake.uuid4(), fake.uuid4()
            )

    async def test_get_chunk_upload_url_returns_presigned_url(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        expected = fake.url()

        httpserver.expect_request('/pilot/upload/gr/v1/files/chunks/presigned').respond_with_json({'result': expected})

        result = await central_node_client.get_chunk_upload_url(
            project_code=fake.project_code(),
            parent_folder_name=fake.user_name(),
            filename=fake.file_name(),
            upload_id=fake.uuid4(),
            chunk_number=1,
        )
        assert result == expected

    async def test_get_chunk_upload_url_raises_on_http_error(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        httpserver.expect_request('/pilot/upload/gr/v1/files/chunks/presigned').respond_with_data(status=404)

        with pytest.raises(httpx.HTTPStatusError):
            await central_node_client.get_chunk_upload_url(
                fake.project_code(), fake.user_name(), fake.file_name(), fake.uuid4(), 1
            )

    async def test_upload_chunk_with_retries_returns_response_on_success(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        httpserver.expect_request('/upload', method='PUT').respond_with_data()
        upload_url = httpserver.url_for('/upload')

        async with httpx.AsyncClient() as client:
            response = await central_node_client.upload_chunk_with_retries(
                client, chunk_number=1, data=fake.binary(5), upload_url=upload_url, retries=3
            )

        assert response.status_code == 200

    async def test_upload_chunk_with_retries_raises_after_exhausting_retries(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker
    ):
        for _ in range(3):
            httpserver.expect_ordered_request('/upload', method='PUT').respond_with_data(status=500)
        upload_url = httpserver.url_for('/upload')

        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.HTTPStatusError):
                await central_node_client.upload_chunk_with_retries(
                    client, chunk_number=1, data=fake.binary(5), upload_url=upload_url, retries=3, backoff_factor=0
                )

    async def test_upload_file_to_project_returns_post_upload_result(
        self, central_node_client: CentralNodeClient, httpserver: HTTPServer, fake: Faker, tmp_path: Path
    ):
        project_code = fake.project_code()
        username = central_node_client.username
        presigned_url = httpserver.url_for('/upload')
        expected_result = {'result': {'status': 'CHUNK_UPLOADED'}}
        filename = fake.file_name()
        file_path = tmp_path / filename
        file_path.write_bytes(fake.binary(15))

        httpserver.expect_request('/pilot/portal/v1/files/meta').respond_with_json(
            {'result': [{'id': fake.uuid4(), 'type': 'name_folder', 'name': username}]}
        )
        httpserver.expect_request(f'/pilot/portal/v1/project/{project_code}/files', method='POST').respond_with_json(
            {
                'result': [
                    {
                        'job_id': fake.uuid4(),
                        'payload': {'resumable_identifier': fake.uuid4(), 'item_id': fake.uuid4()},
                    }
                ]
            }
        )
        httpserver.expect_request('/pilot/upload/gr/v1/files/chunks/presigned').respond_with_json(
            {'result': presigned_url}
        )
        httpserver.expect_request('/upload', method='PUT').respond_with_data()
        httpserver.expect_request('/pilot/upload/gr/v1/files', method='POST').respond_with_json(
            {'result': expected_result}
        )

        result = await central_node_client.upload_file_to_project(
            file_path=file_path,
            destination_file_name=filename,
            project_code=project_code,
            chunk_size=4,
            max_concurrent=2,
        )

        assert result == expected_result
