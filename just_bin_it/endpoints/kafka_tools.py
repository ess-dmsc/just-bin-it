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
    options = {
        "bootstrap.servers": ",".join(brokers),
        "group.id": uuid.uuid4(),
        "allow.auto.create.topics": False,
    }
    consumer = Consumer({**options, **kafka_security_config})

    try:
        for topic in topics:
            metadata = consumer.list_topics(topic=topic, timeout=10)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None:
                logging.error("Could not find topic(s): %s", topic)
                return False
            if topic_metadata.error is not None:
                logging.error(
                    "Could not get metadata for topic %s: %s",
                    topic,
                    topic_metadata.error,
                )
                return False
    except KafkaError as error:
        logging.error(
            "Could not get metadata from Kafka (is the broker address " "correct?): %s",
            error,
        )
        return False
    finally:
        consumer.close()

    return True
