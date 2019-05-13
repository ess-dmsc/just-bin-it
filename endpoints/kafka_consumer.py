import logging
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from typing import List


class Consumer:
    """
    Consumes the messages from Kafka.

    This contains the least amount of logic because it is hard to effectively
    mock the Kafka side without making the tests trivial or pointless.
    """

    def __init__(self, brokers: List[str], topics: List[str]):
        """
        Constructor.

        :param brokers: The names of the brokers to connect to.
        :param topics: The names of the data topics.
        """
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=brokers)
            self.topic_partitions = []
            available_topics = self.consumer.topics()

            for t in topics:
                if t not in available_topics:
                    raise Exception(f"Requested topic {t} not available")

                topic = TopicPartition(t, 0)
                self.topic_partitions.append(topic)
                self.consumer.assign([topic])
                self.consumer.seek_to_end(topic)
        except KafkaError as error:
            raise Exception(error)

    def get_new_messages(self):
        """
        Get any new messages.

        :return: The dict containing the messages.
        """
        data = self.consumer.poll(5)
        for tp in self.topic_partitions:
            logging.info(f"{tp.topic} - current position: {self.consumer.position(tp)}")
        return data
