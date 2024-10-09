# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import time
from enum import Enum
from enum import unique
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Set
from typing import Union


def get_timestamp() -> int:
    """Return current timestamp."""

    return round(time.time())


def append_suffix_to_filepath(filename: str, suffix: Union[str, int], separator: str = '_') -> str:
    """Append suffix to filepath before extension."""

    path = Path(filename)

    current_extension = ''.join(path.suffixes)
    new_extension = f'{separator}{suffix}{current_extension}'

    filename_parts = [path.name, '']
    if current_extension:
        filename_parts = path.name.rsplit(current_extension, 1)

    filename = new_extension.join(filename_parts)

    filepath = str(path.parent / filename)

    return filepath


@unique
class ResourceType(str, Enum):
    """Store all possible types of resources."""

    FOLDER = 'folder'
    FILE = 'file'
    CONTAINER = 'Container'


@unique
class ZoneType(str, Enum):
    GREENROOM = 0
    CORE = 1


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


class Node(dict):
    """Store information about one node."""

    def __str__(self) -> str:
        return f'{self.id} | {self.name}'

    def __dict__(self) -> Dict[str, Any]:
        return self

    @property
    def parent(self) -> str:
        return self['parent']

    @property
    def parent_path(self) -> str:
        return self['parent_path']

    @property
    def id(self) -> str:
        return self['id']

    @property
    def is_folder(self) -> bool:
        return ResourceType.FOLDER == self['type']

    @property
    def is_file(self) -> bool:
        return ResourceType.FILE == self['type']

    @property
    def is_archived(self) -> bool:
        return ItemStatus.ARCHIVED == self['status']

    @property
    def status(self) -> ItemStatus:
        return ItemStatus(self['status'])

    @property
    def name(self) -> str:
        return self['name']

    @property
    def tags(self) -> List[str]:
        return self['extended']['extra']['tags']

    @property
    def size(self) -> int:
        return self.get('size', 0)

    @property
    def container_code(self) -> str:
        return self.get('container_code')

    @property
    def owner(self) -> str:
        return self.get('owner')

    @property
    def namespace(self) -> str:
        result = {1: 'Core'}.get(self.get('zone'), 'Greenroom')
        return result

    @property
    def zone(self) -> int:
        return self.get('zone')

    @property
    def entity_type(self) -> str:
        return self.get('type')

    @property
    def restore_path(self) -> str:
        return self.get('restore_path', '')

    @property
    def display_path(self) -> Path:
        if self['parent_path']:
            full_path = f'{self["parent_path"]}/{self["name"]}'
        else:
            full_path = self['name']
        display_path = Path(full_path)

        if display_path.is_absolute():
            display_path = display_path.relative_to('/')

        return display_path

    def get_attributes(self) -> Dict[str, Any]:
        return self['extended']['extra'].get('attributes', {})


class NodeList(list):
    """Store list of Nodes."""

    def __init__(self, nodes: List[Dict[str, Any]]) -> None:
        super().__init__([Node(node) for node in nodes])

    @property
    def ids(self) -> Set[str]:
        return {node.id for node in self}

    def filter_files(self) -> 'NodeList':
        return NodeList([node for node in self if node.is_file])


class NodeToRegister:
    """Object to store the registered nodes info."""

    def __init__(self, source_node: Node, destination_node: Node) -> None:
        self.source_node = source_node
        self.destination_node = destination_node
