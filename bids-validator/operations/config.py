# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import logging
from functools import lru_cache
from typing import Any

from pydantic import BaseSettings
from pydantic import Extra


class Settings(BaseSettings):
    """Store service configuration settings."""

    APP_NAME: str = 'bids_validator'

    LOGGING_LEVEL: int = logging.INFO
    LOGGING_FORMAT: str = 'json'

    S3_HOST: str = ''
    S3_PORT: int = 9000
    S3_INTERNAL_HTTPS: bool = False
    S3_ACCESS_KEY: str = 'ACCESSKEY/CENLISDWYLOSADDFGMWZ'
    S3_SECRET_KEY: str = 'SECRETKEY/DTRBFII/NJXVSJPLZMGPPVLELR'

    DATAOPS_SERVICE: str = 'http://127.0.0.1:5063'
    QUEUE_SERVICE: str = 'http://127.0.0.1:6060'
    METADATA_SERVICE: str = 'http://127.0.0.1:5066'
    DATASET_SERVICE: str = 'http://127.0.0.1:5081'

    def __init__(self, *args: Any, **kwds: Any) -> None:
        super().__init__(*args, **kwds)

        self.DATAOPS_SERVICE += '/v2/'
        self.QUEUE_SERVICE += '/v1/'
        self.METADATA_SERVICE = self.METADATA_SERVICE + '/v1/'
        self.S3_URL = f'{self.S3_HOST}:{self.S3_PORT}'

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow


@lru_cache
def get_settings():
    settings = Settings()
    return settings


ConfigClass = Settings()
