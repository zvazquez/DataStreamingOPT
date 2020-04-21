import requests
import logging
from confluent_kafka import avro
from utils import prepare_logging
from producers.models.producer import Producer
import json
import random
from enum import IntEnum

logger = prepare_logging(__name__, logging.INFO)

class Weather(Producer):
    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://rest-proxy:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        self.key_schema = avro.load("producers/models/schemas/weather_key.json")
        self.value_schema = avro.load("producers/models/schemas/weather_value.json")

        self.topic_name = "com.opt.weather"
        super().__init__(
            self.topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            num_partitions=1,
            num_replicas=1,
            recreate=False,
        )
        
    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        key = {"timestamp": self.time_millis()}
        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        record = {"temperature": self.temp,
                  "status": self.status.name
                  }
        data = {
            "key_schema": json.dumps(self.key_schema.to_json()),
            "value_schema": json.dumps(self.value_schema.to_json()),
            "records": [{"key": key, "value": record}]
        }
        logger.info(data)
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            data=json.dumps(data),
            headers=headers,
        )
        try:
            resp.raise_for_status()
            logger.info(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")
        except:
            logger.error(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")



