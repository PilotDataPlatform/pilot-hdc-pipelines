# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from operations.services.notification.client import NotificationServiceClient
from operations.services.notification.models import InvolvementType
from operations.services.notification.models import NotificationType
from operations.services.notification.models import PipelineAction
from operations.services.notification.models import PipelineStatus


class TestNotificationServiceClient:
    def test_get_priority_returns_involvement_dictionary_for_all_participants(self, create_node):
        source_folder = create_node(parent_path='user1')
        destination_folder = create_node(parent_path='user2')
        client = NotificationServiceClient(
            '',
            {},
            source_folder,
            destination_folder,
            '',
            PipelineAction.COPY,
            PipelineStatus.SUCCESS,
            'admin',
            NotificationType.PIPELINE,
        )

        expected_priority = {
            InvolvementType.INITIATOR: 'admin',
            InvolvementType.OWNER: 'user1',
            InvolvementType.RECEIVER: 'user2',
        }

        received_priority = client.get_priority()

        assert received_priority == expected_priority
