# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import os
import sys

# TODO: Fix imports as a part of https://indocconsortium.atlassian.net/browse/PILOT-703
sys.path.insert(0, f'{os.getcwd()}/scripts')


def test_send_message_makes_request_to_the_queue_service(mocker, httpserver):
    from config import ConfigClass
    from validate_dataset import send_message

    mocker.patch.object(ConfigClass, 'QUEUE_SERVICE', httpserver.url_for('/'))
    httpserver.expect_oneshot_request('/broker/pub', method='POST').respond_with_json({})

    send_message('dataset-code', 'init', {})

    httpserver.check()


def test_get_files_get_correct_result(mocker, httpserver, create_node):
    from config import ConfigClass
    from models import ResourceType
    from validate_dataset import get_files

    expected_body = {'result': [create_node(type_=ResourceType.FILE, location_uri='minio_path')]}

    mocker.patch.object(ConfigClass, 'METADATA_SERVICE', httpserver.url_for('/'))
    httpserver.expect_oneshot_request('/items/search/', method='GET').respond_with_json(expected_body)

    received_response = get_files('dataset-code', 'access_token')
    assert received_response == ['minio_path']
