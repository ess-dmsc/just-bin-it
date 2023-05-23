import logging
import time
import uuid

from confluent_kafka import Consumer, KafkaException as KafkaError

def are_kafka_settings_valid(brokers, topics):
    """
    Check to see if the broker(s) and topics exist.

    :param brokers: The broker names.
    :return: True if they exist.
    """
    consumer = _are_brokers_present(brokers)
    if consumer is None:
        return False

    return _are_topics_present(consumer, topics)


def _are_brokers_present(brokers):
    try:
        return Consumer({"bootstrap.servers": ",".join(brokers), "group.id": uuid.uuid4()})
    except KafkaError as error:
        logging.error("Could not connect to Kafka brokers: %s", error)
        return None


def _are_topics_present(consumer, topics):
    result = True
    try:
        metadata = consumer.list_topics(timeout=10)
        existing_topics = set(metadata.topics.keys())
        logging.error(existing_topics)
        for tp in topics:
            if tp not in existing_topics:
                logging.error("Could not find topic: %s", tp)
                result = False
    except KafkaError as error:
        logging.error("Could not get topics from Kafka: %s", error)
        return False

    return result