import logging
import uuid

from confluent_kafka import Consumer
from confluent_kafka import KafkaException as KafkaError


def are_kafka_settings_valid(brokers, topics, kafka_security_config):
    """
    Check to see if it is possible to connect to the broker(s) and the topics exist.

    :param brokers: List of broker names.
    :param topics: List of topics.
    :param kafka_security_config: The security config for Kafka.
    :return: True if settings valid.
    """
    # The Consumer constructor does not throw even if the brokers don't exist!
    options = {"bootstrap.servers": ",".join(brokers), "group.id": uuid.uuid4()}
    consumer = Consumer({**options, **kafka_security_config})

    try:
        metadata = consumer.list_topics(timeout=10)
    except KafkaError as error:
        logging.error(
            "Could not get metadata from Kafka (is the broker address " "correct?): %s",
            error,
        )
        return False

    missing_topics = [tp for tp in topics if tp not in set(metadata.topics.keys())]

    if missing_topics:
        logging.error("Could not find topic(s): %s", ", ".join(missing_topics))
        return False

    return True
