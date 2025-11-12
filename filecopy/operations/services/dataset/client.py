# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from typing import Any
from uuid import UUID

from operations.logger import logger
from requests import Session


class DatasetServiceClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = f'{endpoint}/v1/'
        self.client = Session()

    def get_dataset_version(self, version_id: UUID) -> dict[str, Any]:
        try:
            response = self.client.get(f'{self.endpoint}dataset/versions/{version_id}/')
            response.raise_for_status()
            return response.json()
        except Exception:
            logger.exception(f'Failed to get dataset version {version_id}.')
            raise
