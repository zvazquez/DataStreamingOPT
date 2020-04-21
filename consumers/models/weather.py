from utils import prepare_logging
import logging
logger = prepare_logging(__name__, logging.INFO)
import json



class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        try:
            self.value = json.loads(message.value())
            self.temperature = self.value.get('temperature')
            self.status = self.value.get('status')
        except Exception as e:
            logger.error(e)


