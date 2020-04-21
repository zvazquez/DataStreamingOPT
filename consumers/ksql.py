from utils import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)


import requests
import json

from consumers.topic_check import topic_exists

KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'com.opt.turnstile',
    VALUE_FORMAT = 'AVRO',
    KEY = 'station_id'
);
CREATE TABLE SUMMARY
WITH (VALUE_FORMAT = 'JSON') AS
    SELECT COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_exists("SUMMARY") is True:
        logger.info("KSQL tables already exist")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json", "Accept": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()