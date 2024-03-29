from confluent_kafka import KafkaException as KafkaError
from confluent_kafka import Producer as KafkaProducer

from just_bin_it.exceptions import KafkaException


class Producer:
    """
    Publishes messages to Kafka.

    This contains the least amount of logic because it is hard to effectively
    mock the Kafka side without making the tests trivial or pointless.
    """

    def __init__(self, brokers, security_config):
        """
        Constructor.

        :param brokers: The brokers to connect to.
        :param security_config: The security configuration for Kafka.
        """
        try:
            options = {
                "bootstrap.servers": ",".join(brokers),
                "message.max.bytes": 100_000_000,
            }
            self.producer = KafkaProducer({**options, **security_config})
        except KafkaError as error:
            raise KafkaException(error)

    def publish_message(self, topic, message):
        """
        Publish messages into Kafka.

        :param topic: The topic to publish to.
        :param message: The message to publish.
        """
        try:
            self.producer.produce(topic, message)
            self.producer.flush()
        except KafkaError as error:
            raise KafkaException(error)
