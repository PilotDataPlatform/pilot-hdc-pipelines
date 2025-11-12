# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from operations.logger import logger
from operations.models import Node
from operations.services.notification.models import InvolvementType
from operations.services.notification.models import Location
from operations.services.notification.models import NotificationType
from operations.services.notification.models import PipelineAction
from operations.services.notification.models import PipelineNotification
from operations.services.notification.models import PipelineStatus
from operations.services.notification.models import Target
from operations.services.notification.models import TargetType
from requests import Session


class NotificationServiceClient:
    """Client for sending notifications into Notification Service."""

    def __init__(
        self,
        endpoint: str,
        include_nodes: dict[str, Node],
        source_folder: Node,
        destination_folder: Node | None,
        project_code: str,
        pipeline_action: PipelineAction,
        pipeline_status: PipelineStatus,
        operator: str,
        notification_type: NotificationType,
    ) -> None:
        self.endpoint = f'{endpoint}/v1/all/notifications/'
        self.include_nodes = include_nodes
        self.source_folder = source_folder
        self.destination_folder = destination_folder
        self.project_code = project_code
        self.pipeline_action = pipeline_action
        self.pipeline_status = pipeline_status
        self.operator = operator
        self.notification_type = notification_type
        self.client = Session()

    def set_status(self, status: str) -> None:
        self.pipeline_status = status

    def set_location(self, entity: Node) -> Location:
        return Location(id=entity.id, path=str(entity.display_path), zone=entity.zone)

    def set_targets(self) -> list[Target]:
        targets = []
        for _node, file_node in self.include_nodes.items():
            targets.append(Target(id=file_node.id, name=file_node.name, type=TargetType(file_node.entity_type)))
        return targets

    def get_priority(self) -> dict[InvolvementType, str]:
        involvers = {InvolvementType.INITIATOR: self.operator}
        owner = self.source_folder.display_path.parts[0]
        receiver = self.destination_folder.display_path.parts[0] if self.destination_folder else None
        if owner != self.operator:
            involvers[InvolvementType.OWNER] = owner
        if receiver not in [owner, self.operator, None]:
            involvers[InvolvementType.RECEIVER] = receiver
        return involvers

    def send_notifications(self) -> None:
        """Calling notification service API to create notifications."""
        source_folder = self.set_location(self.source_folder)
        targets_entity = self.set_targets()
        involvers = self.get_priority()
        payload = []
        destination_folder = self.set_location(self.destination_folder) if self.destination_folder else None
        for involvement, username in involvers.items():
            notification = PipelineNotification(
                type=NotificationType.PIPELINE,
                recipient_username=username,
                involved_as=involvement,
                action=self.pipeline_action,
                status=self.pipeline_status,
                initiator_username=self.operator,
                project_code=self.project_code,
                source=source_folder,
                destination=destination_folder,
                targets=targets_entity,
            ).to_json()
            payload.append(notification)
        response = self.client.post(self.endpoint, json=payload)
        if response.status_code != 204:
            logger.error(f'Failed to create notification for file {self.pipeline_action}')
            raise Exception(f'Unable to create notifications for file {self.pipeline_action}')
