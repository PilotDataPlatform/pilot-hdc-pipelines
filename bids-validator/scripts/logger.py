# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import logging

from common import configure_logging
from scripts.config import get_settings

settings = get_settings()

logger = logging.getLogger('pilot.bids-validator')

configure_logging(settings.LOGGING_LEVEL, settings.LOGGING_FORMAT)
