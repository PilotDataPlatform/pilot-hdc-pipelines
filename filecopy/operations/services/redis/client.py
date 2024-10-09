# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio

from aioredis import StrictRedis
from operations.config import get_settings

REDIS_INSTANCE = {}


class RedisClient:
    """we should replace StrictRedis with aioredis https://aioredis.readthedocs.io/en/latest/getting-started/"""

    def __init__(self):
        settings = get_settings()
        self.loop = asyncio.new_event_loop()

        self.host = settings.REDIS_HOST
        self.port = settings.REDIS_PORT
        self.pwd = settings.REDIS_PASSWORD
        self.connect()

    def __del__(self):
        self.loop.close()

    def connect(self):
        global REDIS_INSTANCE
        if REDIS_INSTANCE:
            self.__instance = REDIS_INSTANCE
            pass
        else:
            REDIS_INSTANCE = StrictRedis(host=self.host, port=self.port, password=self.pwd)
            self.__instance = REDIS_INSTANCE

    def set_by_key(self, key: str, content: str, expire_time: int = 86400):
        return self.loop.run_until_complete(self.__instance.set(key, content, ex=expire_time))

    def check_by_key(self, key: str):
        return self.loop.run_until_complete(self.__instance.exists(key))

    def delete_by_key(self, key: str):
        return self.loop.run_until_complete(self.__instance.delete(key))
