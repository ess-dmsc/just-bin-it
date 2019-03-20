from kafka import KafkaProducer
from kafka.errors import KafkaError


class Producer:
    """
    Publishes messages to Kafka.

    This contains the least amount of logic because it is hard to effectively
    mock the Kafka side without making the tests trivial or pointless.
    """

    def __init__(self, brokers):
        """
        Constructor.

        :param brokers: The brokers to connect to.
        """
        try:
            self.producer = KafkaProducer(bootstrap_servers=brokers)
        except KafkaError as error:
            raise Exception(error)

    def publish_message(self, topic, message):
        """
        Publish messages into Kafka.

        :param topic: The topic to publish to.
        :param message: The message to publish.
        """
        try:
            self.producer.send(topic, message)
        except KafkaError as error:
            raise Exception(error)
