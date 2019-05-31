import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def are_kafka_settings_valid(brokers, topics):
    """
    Check to see if the broker(s) and topics exist.

    :param brokers: The broker names.
    :return: True if they exist.
    """

    try:
        consumer = KafkaConsumer(bootstrap_servers=brokers)
    except KafkaError as error:
        logging.error(f"Could not connect to Kafka brokers: {error}")
        return False

    result = True
    try:
        existing_topics = consumer.topics()
        for tp in topics:
            if tp not in existing_topics:
                logging.error(f"Could not find topic: {tp}")
                result = False
    except KafkaError as error:
        logging.error(f"Could not get topics from Kafka: {error}")
        return False

    return result
