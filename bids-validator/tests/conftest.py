# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import random
import uuid
from collections.abc import Callable
from typing import Any

import faker
import pytest
from operations.models import ItemStatus
from operations.models import ResourceType


class Faker(faker.Faker):
    def name(self) -> str:
        return self.pystr(max_chars=10)


@pytest.fixture
def fake() -> Faker:
    yield Faker()


@pytest.fixture
def create_node(fake) -> Callable[..., dict[str, Any]]:
    def _create_node(
        id_=None,
        parent=None,
        parent_path=None,
        name=None,
        type_=None,
        zone=0,
        size=0,
        owner='testuser',
        container_code='testproject',
        container_type='project',
        created_time='2021-05-17 17:19:51.806591',
        last_updated_time='2021-05-17 17:19:51.806591',
        status=ItemStatus.ACTIVE,
        tags=None,
        attributes=None,
        location_uri='minio_path',
        version='fake_version',
    ) -> dict[str, Any]:
        if id_ is None:
            id_ = fake.pyint()

        if parent is None:
            parent = f'{uuid.uuid4()}'

        if parent_path is None:
            parent_path = 'fake'

        if name is None:
            name = fake.word()

        if type_ is None:
            type_ = random.choice(list(ResourceType))

        if tags is None:
            tags = fake.words()

        if attributes is None:
            attributes = {}

        return {
            'id': id_,
            'parent': parent,
            'parent_path': parent_path,
            'restore_path': 'None',
            'status': status,
            'type': type_,
            'zone': zone,
            'name': name,
            'size': size,
            'owner': owner,
            'container_code': container_code,
            'container_type': container_type,
            'created_time': created_time,
            'last_updated_time': last_updated_time,
            'storage': {'id': 'fake_id', 'location_uri': location_uri, 'version': version},
            'extended': {
                'id': 'fake_id',
                'extra': {
                    'tags': tags,
                    'system_tags': ['copied-to-core'],
                    'attributes': attributes,
                },
            },
        }

    return _create_node
