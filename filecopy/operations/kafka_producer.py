# Copyright (C) 2022-Present Indoc Systems
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE,
# Version 3.0 (the "License") available at https://www.gnu.org/licenses/agpl-3.0.en.html.
# You may not use this file except in compliance with the License.

import asyncio
import io
from datetime import datetime
from typing import Optional

from aiokafka import AIOKafkaProducer
from fastavro import schema
from fastavro import schemaless_writer
from operations.config import get_settings
from operations.logger import logger
from operations.models import Node

ConfigClass = get_settings()


class KafkaProducer:
    endpoint = ConfigClass.KAFKA_URL
    topic = 'metadata.items.activity'
    schema = 'operations/item_activity_schema.avsc'
    producer = None

    @classmethod
    async def init_connection(self):
        if self.producer is None:
            logger.info('Initializing the kafka producer')
            self.producer = AIOKafkaProducer(bootstrap_servers=self.endpoint, enable_idempotence=True)
            try:
                # Get cluster layout and initial topic/partition leadership information
                await self.producer.start()
            except Exception as e:
                logger.error(f'Fail to start kafka producer:{str(e)}')
                raise e

    @classmethod
    def close_connection(self) -> None:
        if self.producer is not None:
            logger.info('Closing the kafka producer')
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.producer.stop())

    @classmethod
    async def create_file_operation_logs(
        self, input_file: Node, operation_type: str, operator: str, output_file: Optional[Node]
    ):
        message = {
            'activity_type': operation_type,
            'activity_time': datetime.utcnow(),
            'item_id': input_file.id,
            'item_type': input_file.get('type'),
            'item_name': input_file.name,
            'item_parent_path': ('' if input_file.parent_path is None else input_file.parent_path),
            'container_code': input_file.get('container_code'),
            'container_type': input_file.get('container_type'),
            'zone': input_file.get('zone'),
            'user': operator,
            'imported_from': '',
            'changes': [],
        }
        if operation_type == 'delete':
            message['item_parent_path'] = input_file.restore_path

        if operation_type == 'copy':
            message['changes'] = [
                {
                    'item_property': 'path',
                    'old_value': str(input_file.display_path),
                    'new_value': str(output_file.display_path),
                },
                {'item_property': 'id', 'old_value': input_file.id, 'new_value': output_file.id},
            ]
        try:
            # Validate message
            bio = io.BytesIO()
            SCHEMA = schema.load_schema(self.schema)
            schemaless_writer(bio, SCHEMA, message)

            validated_message = bio.getvalue()

            # Send message to kafka
            await self.producer.send_and_wait(self.topic, validated_message)
        except Exception as e:
            logger.error(f'Fail to send message:{str(e)}')
            raise Exception(f'Error when validate and send message to kafka producer: {e}')
