import logging
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from typing import List


class Consumer:
    """
    Consumes the messages from Kafka.

    This contains the least amount of logic so as to make mocking and testing
    easier.
    """

    def __init__(self, brokers: List[str], topics: List[str]):
        """
        Constructor.

        :param brokers: The names of the brokers to connect to.
        :param topics: The names of the data topics.
        """
        try:
            self.consumer = self._create_consumer(brokers)
            self.topic_partitions = []
            self._create_topics(topics)
        except KafkaError as error:
            raise Exception(error)

    def _create_consumer(self, brokers):
        return KafkaConsumer(bootstrap_servers=brokers)

    def _create_topics(self, topics):
        available_topics = self.consumer.topics()

        for t in topics:
            if t not in available_topics:
                raise Exception(f"Requested topic {t} not available")

            topic = TopicPartition(t, 0)
            self.topic_partitions.append(topic)
            self.consumer.assign([topic])
            self.consumer.seek_to_end(topic)

    def _get_new_messages(self):
        data = self.consumer.poll(5)
        for tp in self.topic_partitions:
            logging.debug(
                "%s - current position: %s", tp.topic, self.consumer.position(tp)
            )
        return data

    def get_new_messages(self):
        """
        Get any new messages.

        :return: The dict containing the messages.
        """
        return self._get_new_messages()

    def offset_for_time(self, start_time: int):
        """
        Find the offset to the position corresponding to the supplied time.

        :param start_time: Time to seek in microseconds.
        :return: The offset number.
        """
        return self._offset_for_time(start_time)

    def _offset_for_time(self, start_time):
        # TODO: what about more than one topic?
        answer = self.consumer.offsets_for_times({self.topic_partitions[0]: start_time})
        if answer[self.topic_partitions[0]] is None:
            # Either the topic is empty or the requested time is greater than
            # highest message time in the topic.
            return None
        return answer[self.topic_partitions[0]].offset

    def seek_by_offset(self, offset):
        """
        Move to the specified offset.

        :param offset: The requested offset
        :return:
        """
        self._seek_by_offset(offset)

    def _seek_by_offset(self, offset):
        # TODO: what about more than one topic?
        self.consumer.seek(self.topic_partitions[0], offset)

    def get_offset_range(self):
        """
        Get the lowest and highest offsets for the topic.

        :return: Tuple of lowest and highest offsets.
        """
        return self._get_offset_range()

    def _get_offset_range(self):
        # TODO: what about more than one topic?
        lowest = self.consumer.beginning_offsets(self.topic_partitions)[
            self.topic_partitions[0]
        ]
        highest = self.consumer.end_offsets(self.topic_partitions)[
            self.topic_partitions[0]
        ]
        return lowest, highest
