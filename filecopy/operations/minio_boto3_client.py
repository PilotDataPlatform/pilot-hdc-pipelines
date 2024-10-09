# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import math
import os
import shutil

from common.object_storage_adaptor.boto3_client import Boto3Client
from common.object_storage_adaptor.boto3_client import get_boto3_client
from operations.logger import logger


class MinioBoto3Client:
    def __init__(self, access_key: str, secret_key: str, minio_endpoint: str, minio_https: bool) -> None:
        self.minio_access_key = access_key
        self.minio_secret_key = secret_key
        self.minio_endpoint = minio_endpoint
        self.minio_https = minio_https
        self.client = self.connect_to_minio()

    def connect_to_minio(self) -> Boto3Client:
        loop = asyncio.get_event_loop()
        boto3_client = loop.run_until_complete(
            get_boto3_client(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                https=self.minio_https,
            )
        )
        return boto3_client

    async def download_object(self, src_bucket, src_path, temp_file_path):
        await self.client.download_object(src_bucket, src_path, temp_file_path)

    async def copy_object(self, dest_bucket, dest_path, source_bucket, source_path):
        result = await self.client.copy_object(source_bucket, source_path, dest_bucket, dest_path)
        return result

    async def upload_object(self, bucket, object_name, file_path, temp_path):
        # Here get the total size of the size and set up the max size of each chunk
        try:
            max_size = 5 * 1024 * 1024
            size = os.path.getsize(file_path)
            logger.info(f'File total size is {size}')
            total_parts = math.ceil(size / max_size)

            # Get upload id from upload pre
            upload_id_list = await self.client.prepare_multipart_upload(bucket, [object_name])
            upload_id = upload_id_list[0]
            logger.info(f'The upload id is {upload_id}')
            parts = []
            with open(file_path, 'rb') as f:
                for part_number in range(total_parts):
                    # Cut file into chunks and upload the chunk
                    file_data = f.read(max_size)
                    chunk_result = await self.client.part_upload(
                        bucket, object_name, upload_id, part_number + 1, file_data
                    )
                    parts.append(chunk_result)
                    logger.info(f'Chunk upload result {chunk_result}')
            res = await self.client.combine_chunks(bucket, object_name, upload_id, parts)
            logger.info(f'Finalize the large file upload with version is {res}')
        except Exception:
            raise Exception('File upload failed')
        finally:
            shutil.rmtree(temp_path)
        return res

    async def remove_object(self, src_bucket, src_obj_path):
        result = await self.client.delete_object(src_bucket, src_obj_path)
        return result
