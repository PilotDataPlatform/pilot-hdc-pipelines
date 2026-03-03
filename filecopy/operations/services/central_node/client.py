# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import math
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import jwt as pyjwt
from operations.logger import logger


class CentralNodeClient:

    def __init__(
        self, *, endpoint: str, access_token: str, session_id: str, timeout: int = 30, upload_timeout: int = 300
    ) -> None:
        self.endpoint = endpoint
        self.access_token = access_token
        self.decoded_token = pyjwt.decode(access_token, options={'verify_signature': False}, algorithms=['RS256'])
        self.username = self.decoded_token['preferred_username']
        self.session_id = session_id
        self.timeout = timeout
        self.upload_timeout = upload_timeout
        self.greenroom_name_full = 'greenroom'
        self.greenroom_name_short = 'gr'

    @property
    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self.timeout,
            headers={'Authorization': f'Bearer {self.access_token}', 'Session-ID': self.session_id},
        )

    async def get_name_folder_id(self, project_code: str) -> UUID:
        url = f'{self.endpoint}/pilot/portal/v1/files/meta'
        params = {
            'order_by': 'name',
            'order_type': 'asc',
            'zone': self.greenroom_name_full,
            'project_code': project_code,
            'source_type': 'project',
            'archived': 'false',
            'name': self.username,
        }
        async with self.client as client:
            response = await client.get(url, params=params)
            response.raise_for_status()

        folders = response.json()['result']
        for folder in folders:
            if folder['type'] == 'name_folder' and folder['name'] == self.username:
                return UUID(folder['id'])

        raise ValueError(f'Name folder for user "{self.username}" not found')

    async def file_pre_upload(self, project_code: str, filename: str, parent_folder_id: UUID) -> dict[str, Any]:
        url = f'{self.endpoint}/pilot/portal/v1/project/{project_code}/files'
        data = {
            'project_code': project_code,
            'operator': self.username,
            'job_type': 'AS_FILE',
            'data': [{'resumable_filename': filename, 'resumable_relative_path': self.username}],
            'upload_message': '',
            'current_folder_node': '',
            'parent_folder_id': str(parent_folder_id),
        }
        async with self.client as client:
            response = await client.post(url, json=data)
            response.raise_for_status()

        return response.json()['result'][0]

    async def file_post_upload(
        self,
        project_code: str,
        file_id: UUID,
        parent_folder_name: str,
        filename: str,
        total_chunks: int,
        total_bytes: int,
        upload_id: str,
        job_id: str,
    ) -> dict[str, Any]:
        url = f'{self.endpoint}/pilot/upload/gr/v1/files'
        data = {
            'project_code': project_code,
            'item_id': str(file_id),
            'operator': self.username,
            'resumable_identifier': upload_id,
            'job_id': job_id,
            'resumable_filename': filename,
            'resumable_relative_path': parent_folder_name,
            'resumable_total_chunks': total_chunks,
            'resumable_total_size': total_bytes,
        }
        async with self.client as client:
            response = await client.post(url, json=data)
            response.raise_for_status()

        return response.json()['result']

    async def get_chunk_upload_url(
        self, project_code: str, parent_folder_name: str, filename: str, upload_id: str, chunk_number: int
    ) -> str:
        url = f'{self.endpoint}/pilot/upload/{self.greenroom_name_short}/v1/files/chunks/presigned'
        params = {
            'key': f'{parent_folder_name}/{filename}',
            'upload_id': upload_id,
            'chunk_number': chunk_number,
            'bucket': f'{self.greenroom_name_short}-{project_code}',
        }
        async with self.client as client:
            response = await client.get(url, params=params)
            response.raise_for_status()

        return response.json()['result']

    async def upload_chunk_with_retries(
        self,
        client: httpx.AsyncClient,
        chunk_number: int,
        data: bytes,
        upload_url: str,
        retries: int,
        backoff_factor: float = 0.5,
    ) -> httpx.Response:
        for attempt in range(1, retries + 1):
            try:
                response = await client.put(
                    upload_url, content=data, headers={'Content-Type': 'application/octet-stream'}
                )
                response.raise_for_status()
                logger.info(f'Chunk {chunk_number} uploaded successfully.')
                return response
            except (httpx.NetworkError, httpx.TimeoutException):
                if attempt <= retries:
                    wait_time = backoff_factor * (2 ** (attempt - 1))
                    logger.warning(
                        f'Chunk {chunk_number} upload failed (attempt {attempt}/{retries}). '
                        f'Retrying in {wait_time:.1f} seconds.'
                    )
                    await asyncio.sleep(wait_time)
            except Exception:
                logger.exception(f'Failed to upload chunk {chunk_number}.')
                raise

        logger.error(f'Chunk {chunk_number} failed after {retries} attempts.')
        raise

    async def upload_file_to_project(
        self,
        file_path: Path,
        destination_file_name: str,
        project_code: str,
        chunk_size: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        destination_folder_name = self.username
        destination_name_folder_id = await self.get_name_folder_id(project_code)
        file_pre_upload_data = await self.file_pre_upload(
            project_code, destination_file_name, destination_name_folder_id
        )
        job_id = file_pre_upload_data['job_id']
        upload_id = file_pre_upload_data['payload']['resumable_identifier']
        file_id = UUID(file_pre_upload_data['payload']['item_id'])

        semaphore = asyncio.Semaphore(max_concurrent)

        async def upload_chunk(
            client: httpx.AsyncClient,
            chunk_number: int,
            data: bytes,
        ) -> httpx.Response:
            async with semaphore:
                try:
                    upload_url = await self.get_chunk_upload_url(
                        project_code, destination_folder_name, destination_file_name, upload_id, chunk_number
                    )
                except Exception:
                    logger.exception(f'Failed to get upload url for chunk {chunk_number}.')
                    raise

                return await self.upload_chunk_with_retries(client, chunk_number, data, upload_url, retries=3)

        total_bytes = file_path.stat().st_size
        total_chunks = math.ceil(total_bytes / chunk_size)

        logger.info(
            f'Staring "{destination_file_name}" ({total_bytes} bytes) file upload '
            f'in {total_chunks} chunk(s) of {chunk_size} bytes '
            f'(max {max_concurrent} concurrent) to folder "{destination_folder_name}" '
            f'in the project "{project_code}" on the central node.'
        )

        async with httpx.AsyncClient(timeout=self.upload_timeout) as upload_client:
            tasks = []
            with file_path.open('rb') as f:
                for index in range(total_chunks):
                    chunk_data = f.read(chunk_size)
                    if not chunk_data:
                        break
                    tasks.append(upload_chunk(upload_client, index + 1, chunk_data))

            await asyncio.gather(*tasks)

        return await self.file_post_upload(
            project_code,
            file_id,
            destination_folder_name,
            destination_file_name,
            total_chunks,
            total_bytes,
            upload_id,
            job_id,
        )
