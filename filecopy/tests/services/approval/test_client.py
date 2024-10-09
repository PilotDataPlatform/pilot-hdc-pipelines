# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.


class TestApprovalServiceClient:
    def test_update_copy_status_with_no_error_raised(self, approval_service_client, fake, httpserver):
        entity_id = fake.uuid4()
        expected_body = {'result': [{'entity_id': entity_id}]}
        httpserver.expect_request('/v1/request/request_id/copy-status').respond_with_json(expected_body, status=200)
        result = approval_service_client.update_copy_status(entity_id)

        assert len(result) == 1
