import os
import dotenv
from core.base_message_provider import BaseMessagingConsumer
from core.base_model import ProcessorStatusCode, ProcessorState, MonitorLogEvent
from core.pulsar_messaging_provider import PulsarMessagingConsumerProvider
from db.processor_state_db_storage import PostgresDatabaseStorage, logging

dotenv.load_dotenv()
MSG_URL = os.environ.get("MSG_URL", "pulsar://localhost:6650")
MSG_TOPIC = os.environ.get("MSG_TOPIC", "ism_monitor")
MSG_MANAGE_TOPIC = os.environ.get("MSG_MANAGE_TOPIC", "ism_monitor_manage")
MSG_TOPIC_SUBSCRIPTION = os.environ.get("MSG_TOPIC_SUBSCRIPTION", "ism_monitor_subscription")

# database related
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

# state storage specifically to handle this processor state (stateless obj)
storage = PostgresDatabaseStorage(
    database_url=DATABASE_URL,
    incremental=True
)

messaging_provider = PulsarMessagingConsumerProvider(
    message_url=MSG_URL,
    message_topic=MSG_TOPIC,
    message_topic_subscription=MSG_TOPIC_SUBSCRIPTION,
    management_topic=MSG_MANAGE_TOPIC
)

class MessagingConsumerMonitor(BaseMessagingConsumer):

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

        logging.debug(f'inbound processor state update message received: {message}')
        try:

            # fetch route_id and status
            route_id = message['route_id']
            status = ProcessorStatusCode(message['status'])

            # fetch the stored processor state information
            processor_state = storage.fetch_processor_state_route(route_id=route_id)

            if not processor_state or len(processor_state) != 1:
                raise ValueError(f'invalid processor state for route {route_id}, expected 1 got {processor_state}')

            processor_state = processor_state[0]
            processor_state.status = status    # update the status code
            logging.debug(f'updating processor state {processor_state}')
            processor_state = storage.insert_processor_state_route(processor_state=processor_state)    # persist status
            logging.debug(f'updated processor state {processor_state}')

            # insert monitor log event if any of these are present
            exception = message['exception'] if 'exception' in message else None
            data = message['data'] if 'data' in message else None

            # if there is an exception or data, try to record as much detail
            # as possible, as to the nature of the processor state failure
            if exception or data:
                processor = storage.fetch_processor(processor_id=processor_state.processor_id)
                project = storage.fetch_user_project(processor.project_id)

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
            logging.warn(f'unable to process state update for data: {message}', exc_info=e)

    async def pre_execute(self, consumer_message_mapping: dict, **kwargs):
        pass         # nothing to do here since we do not need to monitor the monitor

    async def post_execute(self, consumer_message_mapping: dict, **kwargs):
        pass         # nothing to do here since we do not need to monitor the monitor

    async def intra_execute(self, consumer_message_mapping: dict, **kwargs):
        pass         # nothing to do here since we do not need to monitor the monitor

    async def fail_validate_input_message(self, consumer_message_mapping: dict, exception: Exception = None):
        logging.error(f'invalid consumer message received: {consumer_message_mapping}', exc_info=exception)

    async def fail_execute_processor_state(
            self, processor_state: ProcessorState,
            exception: Exception,
            data: dict, **kwargs):
        logging.error(f'invalid processor state update received: {processor_state} with data: {data}',
                      exc_info=exception)


if __name__ == '__main__':
    consumer = MessagingConsumerMonitor(
        name="MessagingConsumerMonitor",
        storage=storage,
        messaging_provider=messaging_provider
    )

    consumer.setup_shutdown_signal()
    consumer.start_topic_consumer()
