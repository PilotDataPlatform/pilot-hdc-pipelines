# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from typing import Any
from typing import Dict
from typing import List

from requests import Session


class ApprovalServiceClient:
    """Get information about approval request or entities for copy request."""

    def __init__(self, endpoint: str, request_id: str) -> None:
        self.endpoint = f'{endpoint}/v1'
        self.request_id = request_id
        self.client = Session()

    def update_copy_status(self, entity_id: str) -> List[Dict[str, Any]]:
        """Update copy status field for approval entity."""
        payload = {'entities': [entity_id], 'copy_status': 'copied'}
        response = self.client.put(f'{self.endpoint}/request/{self.request_id}/copy-status', json=payload)
        if response.status_code == 200 and len(response.json()['result']) == 0:
            raise Exception(f'Unable to update copy status for {entity_id}, entity is not found.')
        if response.status_code != 200:
            raise Exception(f'Unable to update copy status for {entity_id}')
        return response.json()['result']
