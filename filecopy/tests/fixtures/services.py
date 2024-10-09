# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import pytest
from operations.services.approval.client import ApprovalServiceClient
from operations.services.dataops.client import DataopsServiceClient
from operations.services.metadata.client import MetadataServiceClient


@pytest.fixture
def dataops_client(httpserver) -> DataopsServiceClient:
    yield DataopsServiceClient(httpserver.url_for('/'))


@pytest.fixture
def metadata_service_client(httpserver) -> MetadataServiceClient:
    yield MetadataServiceClient(
        httpserver.url_for('/'), 'minio-endpoint', 'core-zone', 'temp-dir', httpserver.url_for('/'), 'fake_token'
    )


@pytest.fixture
def approval_service_client(httpserver, fake) -> ApprovalServiceClient:
    request_id = 'request_id'
    yield ApprovalServiceClient(httpserver.url_for('/'), request_id)
