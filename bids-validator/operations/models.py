# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from enum import Enum
from enum import unique


@unique
class ItemStatus(str, Enum):
    # the new enum type for file status
    # - REGISTERED means file is created by upload service
    #   but not complete yet. either in progress or fail.
    # - ACTIVE means file uploading is complete.
    # - ARCHIVED means the file has been deleted

    REGISTERED = 'REGISTERED'
    ACTIVE = 'ACTIVE'
    ARCHIVED = 'ARCHIVED'


@unique
class ResourceType(str, Enum):
    """Store all possible types of resources."""

    FOLDER = 'folder'
    FILE = 'file'
