from kafka import KafkaProducer
from kafka.errors import KafkaError

from just_bin_it.exceptions import KafkaException


class Producer:
    """
    Publishes messages to Kafka.

    This contains the least amount of logic because it is hard to effectively
    mock the Kafka side without making the tests trivial or pointless.
    """

    def __init__(self, brokers, security_config=None):
        """
        Constructor.

        :param brokers: The brokers to connect to.
        :param security_config: Security configuration.
        """
        if security_config is None:
            security_config = {}

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=brokers,
                max_request_size=100_000_000,
                **security_config,
            )
        except KafkaError as error:
            raise KafkaException(error)

    def publish_message(self, topic, message):
        """
        Publish messages into Kafka.

        :param topic: The topic to publish to.
        :param message: The message to publish.
        """
        try:
            self.producer.send(topic, message)
            self.producer.flush()
        except KafkaError as error:
            raise KafkaException(error)
