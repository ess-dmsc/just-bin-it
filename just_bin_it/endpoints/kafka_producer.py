from confluent_kafka import Producer, KafkaException

from just_bin_it.exceptions import KafkaException


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
            self.producer = Producer(
                {'bootstrap.servers': brokers, 'message.max.bytes': 100_000_000}
            )
        except KafkaException as error:
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
        except KafkaException as error:
            raise KafkaException(error)