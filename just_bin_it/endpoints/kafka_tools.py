import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError


def are_kafka_settings_valid(brokers, topics, security_config=None):
    """
    Check to see if the broker(s) and topics exist.

    :param brokers: The broker names.
    :param topics: The topic names.
    :param security_config: Security configuration.
    :return: True if they exist.
    """
    if security_config is None:
        security_config = {}

    consumer = _are_brokers_present(brokers, security_config)
    if consumer is None:
        return False

    return _are_topics_present(consumer, topics)


def _are_brokers_present(brokers, security_config):
    try:
        return KafkaConsumer(bootstrap_servers=brokers, **security_config)
    except KafkaError as error:
        logging.error("Could not connect to Kafka brokers: %s", error)
        return None


def _are_topics_present(consumer, topics):
    result = True
    try:
        existing_topics = consumer.topics()
        for tp in topics:
            if tp not in existing_topics:
                logging.error("Could not find topic: %s", tp)
                result = False
    except KafkaError as error:
        logging.error("Could not get topics from Kafka: %s", error)
        return False

    return result
