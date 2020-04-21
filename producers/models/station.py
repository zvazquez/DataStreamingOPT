import random
import logging
from utils import prepare_logging
from confluent_kafka import avro
from producers.models.producer import Producer
from producers.models.turnstile import Turnstile

logger = prepare_logging(__name__, logging.INFO)

class Station(Producer):
    def __init__(self, station_id, name, color, dir_a=None, dir_b=None):
        self.name = name
        self.station_name = (
            self.name.lower()
                .replace("/", "_and_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("'", "")
        )
        self.key_schema = avro.load("producers/models/schemas/arrival_key.json")
        self.value_schema = avro.load("producers/models/schemas/arrival_value.json")

        
        self.topic_name = "com.opt.station." + self.station_name
        super().__init__(
            self.topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            num_partitions=1,
            num_replicas=1,
            recreate=False,
        )

        self.station_id = int(station_id)
        self.color = color.name
        self.dir_a = dir_a
        self.dir_b = dir_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)

    def train_arrival_info(self, train_id, direction, prev_station_id, prev_direction):
        remove_null = (lambda x : x or '')
        data = {"station_id": self.station_id,
                "train_id": remove_null(str(train_id)),
                "direction": remove_null(direction),
                "line": remove_null(self.color),
                "train_status": random.choice(['on time', 'delayed']),
                "prev_station_id": remove_null(str(prev_station_id)),
                "prev_direction": remove_null(str(prev_station_id)), 
                }

        return data

    def run(self, train, direction, prev_station_id, prev_direction):
        key = {"timestamp":self.time_millis()}
        data = self.train_arrival_info(train, direction, prev_station_id, prev_direction)

        try:
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=data,
                key_schema=self.key_schema,
                value_schema=self.value_schema
            )
            self.producer.poll(0)
            logger.info("Train arrival event logged on {}".format(self.topic_name))
        except BufferError as e:
            logger.error("Buffer full, waiting for free space on the queue")
            self.producer.poll(10)
    
    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)
                
    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)
    
    
    def __del__(self):
        logger.info("Clean object")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.__del__()




                        

