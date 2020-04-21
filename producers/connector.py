from utils import prepare_logging
import json
import logging
import requests
logger = prepare_logging(__name__, logging.INFO)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    logger.info("Creating or Updating Kafka Connector")
    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "1",
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    "connection.user": "cta_admin",
                    "connection.password": "chicago",
                    "table.whitelist": "stations",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": "com.opt.",
                    "poll.interval.ms": "10000",
                    "tasks.max": 1,
                },
            }
        ),
    )
    try:
        resp.raise_for_status()
    except:
        logger.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    logger.info("connector created successfully.")

if __name__ == "__main__":
    configure_connector()
