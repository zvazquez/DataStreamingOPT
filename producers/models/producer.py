"""Producer base-class providing common utilites and functionality"""
import logging
from utils import prepare_logging
import time
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = prepare_logging(__name__, logging.INFO)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
        recreate=False,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.recreate = recreate
        self.broker_url = 'PLAINTEXT://kafka0:9092'
        self.schema_registry_url = 'http://schema-registry:8081'
        self.schema_registry = CachedSchemaRegistryClient({"url": self.schema_registry_url})
        self.client = AdminClient({"bootstrap.servers": self.broker_url})

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": self.broker_url,
            "client.id": "ex4",
            "linger.ms": 1000,
            "compression.type": "gzip",
            "batch.num.messages": 1000000,
        }

        # If the topic does not already exist, try to create it
        if self.topic_exists(self.client, self.topic_name):
            logger.info("Topic {} already exists".format(self.topic_name))
            if self.recreate:
                self.delete_topic(self.client, [self.topic_name])
                time.sleep(5)
                logger.info("Topic {} will be create".format(self.topic_name))
                self.create_topic(self.client)
                Producer.existing_topics.add(self.topic_name)
                
        else:
            logger.info("Topic {} will be create".format(self.topic_name))
            self.create_topic(self.client)
            Producer.existing_topics.add(self.topic_name)
            
            

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=self.schema_registry)

    def delete_topic(self, client, topics):
        """ delete topics """

        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.
        fs = client.delete_topics(topics, operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info("Topic {} deleted".format(topic))                
            except Exception as e:
                logger.error("Failed to delete topic {} with error {}".format(topic, e))
                
                
    def topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


    def create_topic(self, client):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        retention_bytes = 1073741824

        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "retention.bytes": retention_bytes,
                        "compression.type": "gzip",
                        "delete.retention.ms": "1300",
                        "file.delete.delay.ms": "2000",
                    },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info("Topic {} created".format(topic))
            except Exception as e:
                logger.error("Failed to create topic {} with error {}".format(topic, e))
                
        #
        #


    def time_millis(self):
        return int(round(time.time() * 1000))
    
    def __del__(self):
        logger.info("Clean object")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.__del__()
        


