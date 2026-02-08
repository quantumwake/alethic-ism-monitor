import asyncio
import os
import random
import dotenv

from typing import Any
from ismcore.messaging.base_message_provider import BaseMessageConsumer
from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.messaging.base_message_router import Router
from ismcore.messaging.nats_message_provider import NATSMessageProvider
from ismcore.model.base_model import ProcessorStatusCode, MonitorLogEvent, ProcessorState
from ismcore.storage.processor_state_storage import StateMachineStorage
from ismcore.utils.ism_logger import ism_logger
from ismdb.postgres_storage_class import PostgresDatabaseStorage

dotenv.load_dotenv()

# routing file configuration
ROUTING_FILE = os.environ.get("ROUTING_FILE", ".routing-nats.yaml")

# database configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

# storage initialization
storageBackend = PostgresDatabaseStorage(database_url=DATABASE_URL, incremental=True)

# state machine routing
message_provider = NATSMessageProvider()
router = Router(provider=message_provider, yaml_file=ROUTING_FILE)
monitor_route = router.find_route_wildcard("processor/monitor")

logger = ism_logger(__name__)

class MessagingConsumerMonitor(BaseMessageConsumer):

    def __init__(self, storage: StateMachineStorage, route: BaseRoute):
        super().__init__(route=route, monitor_route=None)
        self.storage = storage

    async def execute(self, message: dict):

        if 'type' not in message:
            raise ValueError(f'no state type found in message: {message}')

        message_type = message['type']
        # if not message_type or message_type != 'processor_state':
        #     raise ValueError(f'unsupported monitor type {message_type}')

        # if 'processor_state' not in message:
        #     raise ValueError(f'mandatory processor state information not found in state update message {message}')
        #

        if 'route_id' not in message:
            raise ValueError(f'mandatory route_id value not defined in state update message {message}')

        if 'status' not in message:
            raise ValueError(f'mandatory status value not defined in state update message {message}')

        logger.debug(f'inbound processor state update message received: {message}')
        try:

            # fetch route_id and status
            route_id = message['route_id']
            status = ProcessorStatusCode(message['status'])

            # fetch the stored processor state information
            processor_state = self.storage.fetch_processor_state_route(route_id=route_id)

            if not processor_state or len(processor_state) != 1:
                raise ValueError(f'invalid processor state for route {route_id}, expected 1 got {processor_state}')

            processor_state = processor_state[0]
            logger.debug(f'updating processor state {processor_state.id} status to {status}')
            self.storage.update_processor_state_route_status(route_id=processor_state.id, status=status)
            processor_state.status = status  # update local object to reflect the change
            logger.debug(f'updated processor state {processor_state.id}')

            # insert monitor log event if any of these are present
            exception = message['exception'] if 'exception' in message else None
            data = message['data'] if 'data' in message else None

            # if there is an exception or data, try to record as much detail
            # as possible, as to the nature of the processor state failure
            if exception or data:
                processor = self.storage.fetch_processor(processor_id=processor_state.processor_id)
                project = self.storage.fetch_user_project(processor.project_id)

                # record the event log
                monitor_log_event = MonitorLogEvent(
                    log_type=message_type,
                    internal_reference_id=processor_state.internal_id,
                    user_id=project.user_id,
                    project_id=processor.project_id,
                    data=str(message['data']) if 'data' in message else None,
                    exception=str(message['exception']) if 'exception' in message else None
                )

                self.storage.insert_monitor_log_event(monitor_log_event=monitor_log_event)
        except Exception as e:
            logger.warn(f'unable to process state update for data: {message}', exc_info=e)

    async def pre_execute(self, consumer_message_mapping: dict, **kwargs):
        pass         # nothing to do here since we do not need to monitor the monitor

    async def post_execute(self, consumer_message_mapping: dict, **kwargs):
        pass         # nothing to do here since we do not need to monitor the monitor

    async def intra_execute(self, consumer_message_mapping: dict, **kwargs):
        pass         # nothing to do here since we do not need to monitor the monitor

    async def fail_validate_input_message(self, consumer_message_mapping: dict, exception: Exception = None):
        logger.error(f'invalid consumer message received: {consumer_message_mapping}', exc_info=exception)

    async def fail_execute_processor_state(self,
                                           route_id: str,
                                           exception: Any,
                                           data: dict = None,
                                           **kwargs):
        logger.error(f'invalid processor state update received: {route_id} with data: {data}',
                     exc_info=exception)


if __name__ == '__main__':
    consumer = MessagingConsumerMonitor(
        route=monitor_route,
        storage=storageBackend
    )

    consumer.setup_shutdown_signal()
    consumer_no = random.randint(0, 20)
    asyncio.get_event_loop().run_until_complete(consumer.start_consumer())
