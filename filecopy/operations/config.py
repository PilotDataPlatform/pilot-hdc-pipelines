# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import logging
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import Optional

from common import VaultClient
from pydantic import BaseSettings
from pydantic import Extra


class VaultConfig(BaseSettings):
    """Store vault related configuration."""

    APP_NAME: str = 'pipelines'
    CONFIG_CENTER_ENABLED: bool = False

    VAULT_URL: Optional[str]
    VAULT_CRT: Optional[str]
    VAULT_TOKEN: Optional[str]

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    config = VaultConfig()

    if not config.CONFIG_CENTER_ENABLED:
        return {}

    client = VaultClient(config.VAULT_URL, config.VAULT_CRT, config.VAULT_TOKEN)
    return client.get_from_vault(config.APP_NAME)


class Settings(BaseSettings):
    """Store service configuration settings."""

    APP_NAME: str = 'pipelines'

    LOGGING_LEVEL: int = logging.INFO
    LOGGING_FORMAT: str = 'json'

    S3_HOST: str = ''
    S3_PORT: int = 9000
    S3_INTERNAL_HTTPS: bool = False
    S3_ACCESS_KEY: str = 'ACCESSKEY/CENLISDWYLOSADDFGMWZ'
    S3_SECRET_KEY: str = 'SECRETKEY/DTRBFII/NJXVSJPLZMGPPVLELR'
    S3_URL: str = ''

    DATAOPS_SERVICE: str = 'http://127.0.0.1:5063'
    METADATA_SERVICE: str = 'http://127.0.0.1:5066'
    PROJECT_SERVICE: str = 'http://127.0.0.1:5064'
    APPROVAL_SERVICE: str = 'http://127.0.0.1:8000'
    NOTIFICATION_SERVICE: str = 'http://127.0.0.1:5065'

    GREEN_ZONE_LABEL: str = 'Greenroom'
    CORE_ZONE_LABEL: str = 'Core'

    TEMP_DIR: str = './filecopy'
    COPIED_WITH_APPROVAL_TAG: str = 'copied-to-core'
    REDIS_USER: str = 'default'
    REDIS_PASSWORD: str = ''
    REDIS_HOST: str = '127.0.0.1'
    REDIS_PORT: int = 6379
    REDIS_URL: str = ''
    KAFKA_URL: str = ''

    def __init__(self, *args: Any, **kwds: Any) -> None:
        super().__init__(*args, **kwds)

        self.REDIS_URL = f'redis://{self.REDIS_USER}:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}'
        self.S3_URL = f'{self.S3_HOST}:{self.S3_PORT}'

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.ignore

        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return init_settings, env_settings, load_vault_settings, file_secret_settings


@lru_cache(1)
def get_settings() -> Settings:
    settings = Settings()
    return settings
