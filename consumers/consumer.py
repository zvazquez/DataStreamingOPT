from utils import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.BROKER_URL = 'PLAINTEXT://localhost:9092'
        self.SCHEMA_REGISTRY_URL = 'http://localhost:8081'

        self.broker_properties = {
            "bootstrap.servers": self.BROKER_URL,
            "group.id": "0"
        }

        if is_avro is True:
            schema_registry = CachedSchemaRegistryClient({"url": self.SCHEMA_REGISTRY_URL})
            self.consumer = AvroConsumer(self.broker_properties, schema_registry=schema_registry)
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        while True:
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                return 0
            elif message.error():
                logger.error(message.error())
            else:
                if self.topic_name_pattern == 'com.opt.weather':
                    logger.info(message.value())
                return 1


    def __del__(self):
        pass

    def close(self):
        self.consumer.close()
        self.__del__()