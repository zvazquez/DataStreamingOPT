import random
import logging
from utils import prepare_logging
from confluent_kafka import avro
from producers.models.producer import Producer
from producers.models.turnstile_hardware import TurnstileHardware

logger = prepare_logging(__name__, logging.INFO)

class Turnstile(Producer):
    def __init__(self, station):
        """Create the Turnstile"""
        self.station_name = (
            station.name.lower()
                .replace("/", "_and_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("'", "")
        )

        self.key_schema = avro.load("producers/models/schemas/turnstile_key.json")
        self.value_schema = avro.load("producers/models/schemas/turnstile_value.json")

        self.topic_name = "com.opt.turnstile"
        super().__init__(
            self.topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            num_partitions=1,
            num_replicas=1,
            recreate=False,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def turnstile_info(self, timestamp, time_step):
        data = {"station_id": self.station.station_id,
                "station_name": self.station.station_name,
                "line": self.turnstile_hardware.get_entries(timestamp, time_step)
                }

        return data

    def run(self, timestamp, time_step):
        key = {"timestamp": self.time_millis()}
        data = self.turnstile_info(timestamp, time_step)

        try:

            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=data,
                key_schema=self.key_schema,
                value_schema=self.value_schema
            )
            self.producer.poll(0)
            logger.info("Turnstile event logged on {}".format(self.topic_name))
        except BufferError as e:
            logger.error("Buffer full, waiting for free space on the queue")
            self.producer.poll(10)

