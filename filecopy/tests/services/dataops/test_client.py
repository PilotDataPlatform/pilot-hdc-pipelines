# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from pathlib import Path

from operations.services.dataops.client import JobStatus
from operations.services.dataops.client import ResourceLockOperation


class TestDataopsServiceClient:
    def test_lock_resources_returns_response_body(self, dataops_client, httpserver, fake):
        expected_body = fake.pydict(value_types=['str', 'int'])
        httpserver.expect_request('/v2/resource/lock/bulk').respond_with_json(expected_body)

        received_body = dataops_client.lock_resources([Path('key')], ResourceLockOperation.READ)

        assert received_body == expected_body

    def test_unlock_resources_returns_response_body(self, dataops_client, httpserver, fake):
        expected_body = fake.pydict(value_types=['str', 'int'])
        httpserver.expect_request('/v2/resource/lock/bulk').respond_with_json(expected_body, status=400)

        received_body = dataops_client.unlock_resources([Path('key')], ResourceLockOperation.READ)

        assert received_body == expected_body

    def test_update_job_returns_response_body(self, dataops_client, httpserver, fake):
        expected_body = fake.pydict(value_types=['str', 'int'])
        httpserver.expect_request('/v1/task-stream/').respond_with_json(expected_body)

        received_body = dataops_client.update_job(
            session_id=fake.geid(),
            job_id=fake.geid(),
            target_names=[fake.file_path(depth=3)],
            target_type='file',
            container_code=fake.name(),
            action_type='data_transfer',
            status=JobStatus.SUCCEED,
        )

        assert received_body == expected_body

    def test_get_zip_preview_returns_response_body(self, dataops_client, httpserver, fake):
        expected_body = fake.pydict(value_types=['str', 'int'])
        httpserver.expect_request('/v1/archive').respond_with_json(expected_body)

        received_body = dataops_client.get_zip_preview(fake.geid())

        assert received_body == expected_body

    def test_get_zip_preview_returns_none_when_archive_does_not_exist(self, dataops_client, httpserver, fake):
        httpserver.expect_request('/v1/archive').respond_with_json({}, status=404)

        received_body = dataops_client.get_zip_preview(fake.geid())

        assert received_body is None

    def test_create_zip_preview_returns_response_body(self, dataops_client, httpserver, fake):
        expected_body = fake.pydict(value_types=['str', 'int'])
        httpserver.expect_request('/v1/archive').respond_with_json(expected_body)

        received_body = dataops_client.create_zip_preview(fake.geid(), fake.pydict(value_types=['str']))

        assert received_body == expected_body
