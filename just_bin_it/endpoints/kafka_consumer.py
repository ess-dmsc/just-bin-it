import logging
from typing import List

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from just_bin_it.exceptions import KafkaException


class Consumer:
    """
    Consumes the messages from Kafka.

    This contains the least amount of logic so as to make mocking and testing
    easier.

    Note: Can only handle one topic.
    """

    def __init__(self, brokers: List[str], topics: List[str]):
        """
        Constructor.

        :param brokers: The names of the brokers to connect to.
        :param topics: The names of the data topics.
        """
        self.topic_partitions = []
        try:
            self.consumer = self._create_consumer(brokers)
            self._assign_topics(topics)
        except KafkaError as error:
            raise KafkaException(error)

    def _create_consumer(self, brokers):
        return KafkaConsumer(bootstrap_servers=brokers)

    def _assign_topics(self, topics):
        # Only use the first topic
        topic = topics[0]

        available_topics = self.consumer.topics()

        if topic not in available_topics:
            raise KafkaException(f"Requested topic {topic} not available")

        partition_numbers = self.consumer.partitions_for_topic(topic)

        for pn in partition_numbers:
            self.topic_partitions.append(TopicPartition(topic, pn))

        self.consumer.assign(self.topic_partitions)
        self.consumer.seek_to_end()

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
        partitions = {tp: start_time for tp in self.topic_partitions}
        offsets = self.consumer.offsets_for_times(partitions)
        result = []
        for tp in self.topic_partitions:
            if offsets[tp] is None:
                # Either the topic is empty or the requested time is greater than
                # highest message time in the topic.
                result.append(None)
            else:
                result.append(offsets[tp].offset)
        return result

    def seek_by_offsets(self, offsets):
        """
        Move to the specified offsets.

        :param offsets: The requested offsets.
        """
        self._seek_by_offsets(offsets)

    def _seek_by_offsets(self, offsets):
        for tp, offset in zip(self.topic_partitions, offsets):
            self.consumer.seek(tp, offset)

    def get_offset_range(self):
        """
        Get the lowest and highest offsets for the topic.

        :return: Tuple of lowest and highest offsets.
        """
        return self._get_offset_range()

    def _get_offset_range(self):
        lowest = self.consumer.beginning_offsets(self.topic_partitions)
        highest = self.consumer.end_offsets(self.topic_partitions)
        offset_ranges = []
        for tp in self.topic_partitions:
            offset_ranges.append((lowest[tp], highest[tp]))

        return offset_ranges

    def get_positions(self):
        """
        Get the position of the consumer for each partition.

        :return: List of positions.
        """
        return self._get_positions()

    def _get_positions(self):
        positions = []
        for tp in self.topic_partitions:
            positions.append(self.consumer.position(tp))
        return positions
