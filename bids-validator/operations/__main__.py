# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

from operations.app import app
from operations.commands.validate_dataset import validate_dataset

if __name__ == '__main__':
    app.add_command(validate_dataset)
    app()
