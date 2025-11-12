# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import logging

from operations.app import app
from operations.commands.copy import copy
from operations.commands.delete import delete
from operations.commands.share_dataset_version import share_dataset_version

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s\t%(levelname)s\t[%(name)s]\t%(message)s')

    app.add_command(copy)
    app.add_command(delete)
    app.add_command(share_dataset_version)
    app()
