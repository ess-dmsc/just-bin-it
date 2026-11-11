import logging

from confluent_kafka import KafkaException as KafkaError
from confluent_kafka import Producer as KafkaProducer

from just_bin_it.exceptions import KafkaException

# Don't block on a broker that is down, undelivered messages stay queued
DELIVERY_WAIT_S = 1.0
QUEUE_MAX_KBYTES = 200_000
PREPARE_TIMEOUT_S = 5


def _report_delivery(error, message):
    if error is not None:
        logging.error(
            "Could not publish a message to %s: %s", message.topic(), error
        )  # pragma: no mutate


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
                "queue.buffering.max.kbytes": QUEUE_MAX_KBYTES,
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
            self.producer.produce(topic, message, on_delivery=_report_delivery)
        except BufferError:
            logging.error(
                "Kafka producer queue is full, dropping a message to %s", topic
            )  # pragma: no mutate
            self.producer.poll(0)
            return
        except KafkaError as error:
            raise KafkaException(error)
        self.producer.flush(DELIVERY_WAIT_S)

    def prepare_topic(self, topic):
        """
        Fetch the topic metadata up front, otherwise the first message can be
        delayed by about a second.

        :param topic: The topic that will be published to.
        """
        try:
            self.producer.list_topics(topic, timeout=PREPARE_TIMEOUT_S)
        except Exception as error:
            logging.warning(
                "Could not get the metadata of topic %s: %s", topic, error
            )  # pragma: no mutate

    def flush(self, timeout):
        """
        Wait for the queued messages to be delivered.

        :param timeout: How long to wait at most (seconds).
        :return: The number of messages not delivered.
        """
        return self.producer.flush(timeout)
