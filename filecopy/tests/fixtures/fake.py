# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import faker
import pytest
from common import GEIDClient


class Faker(faker.Faker):
    def geid(self) -> str:
        """Generate global entity id."""

        return GEIDClient().get_GEID()

    def name(self) -> str:
        return Faker().pystr(max_chars=10)


@pytest.fixture
def fake() -> Faker:
    yield Faker()
