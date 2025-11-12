# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from enum import Enum
from enum import unique
from pathlib import Path
from typing import Any

from operations.logger import logger
from requests import Session


@unique
class ResourceLockOperation(str, Enum):
    READ = 'read'
    WRITE = 'write'

    class Config:
        use_enum_values = True


@unique
class JobStatus(str, Enum):
    SUCCEED = 'SUCCEED'
    FAILED = 'FAILED'

    class Config:
        use_enum_values = True


class DataopsServiceClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint_v1 = f'{endpoint}/v1'
        self.endpoint_v2 = f'{endpoint}/v2'
        self.client = Session()

    def lock_resources(self, resource_keys: list[Path], operation: ResourceLockOperation) -> dict[str, Any]:
        resource_keys = list(map(str, resource_keys))

        logger.info(f'Performing "{operation}" lock for resource keys: {resource_keys}.')
        response = self.client.post(
            f'{self.endpoint_v2}/resource/lock/bulk',
            json={
                'resource_keys': resource_keys,
                'operation': operation,
            },
        )

        if response.status_code != 200:
            message = f'Unable to lock resource keys: {resource_keys}.'
            logger.info(message)
            raise Exception(message)

        logger.info(f'Successfully "{operation}" locked resource keys: {resource_keys}.')
        return response.json()

    def unlock_resources(self, resource_keys: list[Path], operation: ResourceLockOperation) -> dict[str, Any]:
        resource_keys = list(map(str, resource_keys))

        logger.info(f'Performing "{operation}" unlock for resource keys: {resource_keys}.')
        response = self.client.delete(
            f'{self.endpoint_v2}/resource/lock/bulk',
            json={
                'resource_keys': resource_keys,
                'operation': operation,
            },
        )

        if response.status_code not in (200, 400):
            message = f'Unable to unlock resource keys: {resource_keys}.'
            logger.info(message)
            raise Exception(message)

        logger.info(f'Successfully "{operation}" unlocked resource keys: {resource_keys}.')
        return response.json()

    def update_job(
        self,
        session_id: str,
        job_id: str,
        target_names: list,
        target_type: list,
        container_code: str,
        action_type: str,
        status: JobStatus,
    ) -> dict[str, Any]:
        """Create task-stream record for updated job status."""
        response = self.client.post(
            f'{self.endpoint_v1}/task-stream/',
            json={
                'session_id': session_id,
                'target_names': target_names,
                'target_type': target_type,
                'container_code': container_code,
                'container_type': 'project',
                'action_type': action_type,
                'job_id': job_id,
                'status': status.name,
            },
        )

        if response.status_code != 200:
            logger.error(
                f'Unexpected status code received while updating job"{job_id}" Received response: "{response.text}".'
            )
            raise Exception(f'Unable to update job "{job_id}".')

        return response.json()

    def get_zip_preview(self, file_geid: str) -> dict[str, Any] | None:
        response = self.client.get(
            f'{self.endpoint_v1}/archive',
            params={
                'file_id': file_geid,
            },
        )
        if response.status_code == 404:
            return None

        if response.status_code != 200:
            logger.error(
                'Unexpected status code received while getting zip preview '
                f'for geid "{file_geid}". '
                f'Received response: "{response.text}".'
            )
            raise Exception(f'Unable to get zip preview for id "{file_geid}".')

        return response.json()

    def create_zip_preview(self, file_id: str, archive_preview: dict[str, Any]) -> dict[str, Any]:
        response = self.client.post(
            f'{self.endpoint_v1}/archive',
            json={
                'file_id': file_id,
                'archive_preview': archive_preview,
            },
        )

        if response.status_code != 200:
            logger.error(
                'Unexpected status code received while creating zip '
                f'preview for geid "{file_id}". '
                f'Received response: "{response.text}".'
            )
            raise Exception(f'Unable to create zip preview for id "{file_id}".')

        return response.json()
