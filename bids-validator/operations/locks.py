# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from typing import Any

import requests
from operations.config import ConfigClass
from operations.models import ItemStatus
from operations.models import ResourceType


def get_all_children_nodes(parent_path: str, zone: str, container_code: str, access_token: str) -> list[dict[str, Any]]:
    item_zone = {'greenroom': 0, 'core': 1}.get(zone)
    parameters = {
        'status': ItemStatus.ACTIVE,
        'zone': item_zone,
        'container_code': container_code,
        'container_type': 'dataset',
        'recursive': True,
    }
    if parent_path:
        parameters['parent_path'] = parent_path

    header = {'Authorization': access_token}

    node_query_url = ConfigClass.METADATA_SERVICE + 'items/search/'
    response = requests.get(node_query_url, params=parameters, headers=header)
    ffs = response.json()['result']

    return ffs


def lock_resource(resource_key: str, operation: str) -> dict:
    # operation can be either read or write
    url = ConfigClass.DATAOPS_SERVICE + 'resource/lock'
    post_json = {'resource_key': resource_key, 'operation': operation}

    response = requests.post(url, json=post_json)
    if response.status_code != 200:
        raise Exception(f'resource {resource_key} already in used')

    return response.json()


def unlock_resource(resource_key: str, operation: str) -> dict:
    # operation can be either read or write
    url = ConfigClass.DATAOPS_SERVICE + 'resource/lock'
    post_json = {'resource_key': resource_key, 'operation': operation}

    response = requests.delete(url, json=post_json)
    if response.status_code != 200:
        raise Exception(f'Error when unlock resource {resource_key}')

    return response.json()


def format_folder_path(node):
    parent_path = node.get('parent_path', None)
    if parent_path:
        return f'{parent_path}/{node.get("name")}'
    return node.get('name')


def lock_nodes(dataset_code: str, access_token: str):
    """The function will recursively lock the node tree."""

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    try:
        nodes = get_all_children_nodes(None, 'core', dataset_code, access_token)
        for ff_object in nodes:
            # we will skip the deleted nodes
            if ff_object.get('archived', False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source and target
            if ff_object.get('type') == ResourceType.FILE:
                location = ff_object.get('storage')['location_uri']
                minio_path = location.split('//')[-1]
                _, bucket, minio_obj_path = tuple(minio_path.split('/', 2))
            else:
                bucket = ff_object.get('container')
                minio_obj_path = format_folder_path(ff_object)

            source_key = f'{bucket}/{minio_obj_path}'
            lock_resource(source_key, 'read')
            locked_node.append((source_key, 'read'))

    except Exception as e:
        err = e

    return locked_node, err
