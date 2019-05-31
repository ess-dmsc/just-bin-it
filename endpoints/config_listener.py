import logging
import time
from endpoints.kafka_consumer import Consumer
from endpoints.kafka_tools import kafka_settings_valid
from endpoints.sources import ConfigSource


class ConfigListener:
    def __init__(self, brokers, config_topic):
        self.brokers = brokers
        self.config_topic = config_topic
        self.config_source = None
        self.message = None

    def connect(self):
        logging.info("Creating configuration consumer")
        while not kafka_settings_valid(self.brokers, [self.config_topic]):
            logging.error(
                f"Could not connect to Kafka brokers or topic for configuration - will retry shortly"
            )
            time.sleep(5)
        config_consumer = Consumer(self.brokers, [self.config_topic])
        self.config_source = ConfigSource(config_consumer)

    def check_for_messages(self):
        """
        Checks for unconsumed messages.

        :return: True if there is a message available.
        """
        messages = self.config_source.get_new_data()
        if messages:
            # Only interested in the "latest" message
            self.message = messages[-1]
            logging.info("New configuration message received")
            return True
        else:
            # Return whether there is a message waiting
            return self.message is not None

    def consume_message(self):
        """
        :return: The waiting message.
        """
        if self.message:
            msg = self.message
            self.message = None
            return msg
        else:
            raise Exception("No message available")
