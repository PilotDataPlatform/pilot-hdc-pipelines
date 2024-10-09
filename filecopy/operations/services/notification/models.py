# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import json
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import conlist


class InvolvementType(str, Enum):
    INITIATOR = 'initiator'
    OWNER = 'owner'
    RECEIVER = 'receiver'


class PipelineAction(str, Enum):
    DELETE = 'delete'
    COPY = 'copy'


class PipelineStatus(str, Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'


class NotificationType(str, Enum):
    PIPELINE = 'pipeline'
    COPY_REQUEST = 'copy-request'


class TargetType(str, Enum):
    FILE = 'file'
    FOLDER = 'folder'


class Location(BaseModel):
    id: UUID
    path: str
    zone: int


class Target(BaseModel):
    id: UUID
    name: str
    type: TargetType


class PipelineNotification(BaseModel):
    type: NotificationType
    recipient_username: str
    involved_as: InvolvementType
    action: PipelineAction
    status: PipelineStatus
    initiator_username: str
    project_code: str
    source: Location
    destination: Optional[Location]
    targets: conlist(Target, min_items=1)

    def to_json(self):
        return json.loads(self.json())
