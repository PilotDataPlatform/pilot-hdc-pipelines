# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import time
import traceback
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List

import requests
from common.object_storage_adaptor.boto3_client import get_boto3_client
from config import ConfigClass
from locks import lock_nodes
from locks import unlock_resource
from models import ItemStatus
from models import ResourceType
from scripts.logger import logger

TEMP_FOLDER = './dataset/'


def parse_inputs() -> dict:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('-d', '--dataset-code', help='Dataset code', required=True)
    parser.add_argument('-env', '--environment', help='Environment', required=True)
    parser.add_argument('--access-token', '--access-token', help='Access Token', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def send_message(dataset_code: str, status: str, bids_output: Dict[str, Any]) -> None:
    queue_url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
    post_json = {
        'event_type': 'BIDS_VALIDATE_NOTIFICATION',
        'payload': {
            'status': status,  # INIT/RUNNING/FINISH/ERROR
            'dataset': dataset_code,
            'payload': bids_output,
            'update_timestamp': datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S'),
        },
        'binary': True,
        'queue': 'socketio',
        'routing_key': 'socketio',
        'exchange': {'name': 'socketio', 'type': 'fanout'},
    }

    if status == 'failed':
        post_json['payload']['payload'] = None
        post_json['payload']['error_msg'] = bids_output

    try:
        queue_res = requests.post(queue_url, json=post_json)
        if queue_res.status_code != 200:
            logger.info(f'code: {queue_res.status_code}: {queue_res.text}')
        queue_res.raise_for_status()
        logger.info('sent message to queue')
        return
    except Exception as e:
        logger.error(f'Failed to send message to queue: {str(e)}')
        raise


def get_files(dataset_code: str, access_token: str) -> List[str]:
    all_files = []

    query = {
        'status': ItemStatus.ACTIVE,
        'zone': 1,
        'container_code': dataset_code,
        'container_type': 'dataset',
        'recursive': True,
    }

    header = {'Authorization': access_token}

    try:
        resp = requests.get(ConfigClass.METADATA_SERVICE + 'items/search/', params=query, headers=header)
        for node in resp.json()['result']:
            if node['type'] == ResourceType.FILE:
                all_files.append(node['storage']['location_uri'])
        return all_files
    except Exception as e:
        logger.error(f'Error when get files: {str(e)}')
        raise


async def download_from_minio(files_locations: List[str]) -> None:
    boto3_client = await get_boto3_client(
        ConfigClass.S3_URL,
        access_key=ConfigClass.S3_ACCESS_KEY,
        secret_key=ConfigClass.S3_SECRET_KEY,
        https=ConfigClass.S3_INTERNAL_HTTPS,
    )
    try:
        for file_location in files_locations:
            minio_path = file_location.split('//')[-1]
            _, bucket, obj_path = tuple(minio_path.split('/', 2))
            await boto3_client.download_object(bucket, obj_path, TEMP_FOLDER + obj_path)

        logger.info('========Minio_Client download finished========')

    except Exception as e:
        logger.error(f'Error when download data from minio: {str(e)}')
        raise


def getProcessOutput() -> None:
    f = open('result.txt', 'w')
    try:
        subprocess.run(['bids-validator', TEMP_FOLDER + 'data', '--json'], universal_newlines=True, stdout=f)
    except Exception as e:
        logger.error(f'BIDS validate fail: {str(e)}')
        raise


def read_result_file() -> str:
    f = open('result.txt', 'r')
    output = f.read()
    return output


def send_result_to_dataset(dataset_code: str, result: Dict[str, Any]) -> str:
    query = {'validate_output': result}
    try:
        response = requests.put(ConfigClass.DATASET_SERVICE + f'/v1/dataset/bids-result/{dataset_code}', json=query)
        logger.info('Submit the result to dataset service')
        if response.status_code != 200:
            logger.error(f'Failed to send result to dataset service {response.text}')
        return
    except Exception as e:
        logger.error(f'Error when submit the result to dataset service: {str(e)}')
        raise


def main():
    logger.info(f'Vault url: {os.getenv("VAULT_URL")}')
    environment = args.get('environment', 'test')
    logger.info(f'environment: {args.get("environment")}')
    logger.info(f'config set: {environment}')
    try:
        # get arguments
        dataset_code = args['dataset_code']
        access_token = args['access_token']

        logger.info(f'dataset_code: {dataset_code}')
        logger.info(f'access_token: {access_token}')

        locked_node = []
        files_locations = get_files(dataset_code, access_token)
        # here add recursive read lock on the dataset
        locked_node, err = lock_nodes(dataset_code, access_token)
        if err:
            raise err

        if len(files_locations) == 0:
            send_message(dataset_code, 'failed', 'no files in dataset')
            return

        # Download files folders from minio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_from_minio(files_locations))
        logger.info('files are downloaded from minio')

        # Get bids validate result
        getProcessOutput()
        result = read_result_file()

        logger.info(f'BIDS validation result: {result}')

        bids_output = json.loads(result)

        # remove bids folder after validate
        shutil.rmtree(TEMP_FOLDER)

        # Send the bids validation result to dataset service
        send_result_to_dataset(dataset_code, bids_output)

        send_message(dataset_code, 'success', bids_output)

    except Exception as e:
        logger.error(f'BIDs validator failed due to: {str(e)}')
        send_message(dataset_code, 'failed', str(e))
        raise

    finally:
        for resource_key, operation in locked_node:
            unlock_resource(resource_key, operation)


if __name__ == '__main__':
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger.error(f'[Validate Failed] {str(e)}')
        for info in traceback.format_stack():
            logger.error(info)
        raise
