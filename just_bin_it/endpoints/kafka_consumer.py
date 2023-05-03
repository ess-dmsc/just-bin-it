import logging
from typing import List

from confluent_kafka import Consumer as KafkaConsumer, TopicPartition, KafkaException as KafkaError, OFFSET_END

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
        return KafkaConsumer({"bootstrap.servers": ','.join(brokers), "group.id": "mygroup"})

    def _assign_topics(self, topics):
        # Only use the first topic
        topic = topics[0]

        metadata = self.consumer.list_topics(timeout=10)
        available_topics = set(metadata.topics.keys())

        if topic not in available_topics:
            raise KafkaException(f"Requested topic {topic} not available")

        partition_numbers = metadata.topics[topic].partitions.keys()

        for pn in partition_numbers:
            self.topic_partitions.append(TopicPartition(topic, pn))

        self.consumer.assign(self.topic_partitions)
        # Seek to the end of each partition
        for tp in self.topic_partitions:
            self.consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))

    def _get_new_messages(self):
        data = self.consumer.consume(timeout=5)
        for tp in self.topic_partitions:
            logging.debug(
                "%s - current position: %s", tp.topic, self.consumer.position([tp])[0].offset
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
            self.consumer.seek(TopicPartition(tp.topic, tp.partition, offset))

    def get_offset_range(self):
        """
        Get the lowest and highest offsets for the topic.

        :return: Tuple of lowest and highest offsets.
        """
        return self._get_offset_range()

    def _get_offset_range(self):
        offset_ranges = []
        for tp in self.topic_partitions:
            try:
                (low, high) = self.consumer.get_watermark_offsets(tp, timeout=1)
                offset_ranges.append((low, high))
            except KafkaError as error:
                logging.error("Could not get watermark offsets for topic-partition %s: %s", tp, error)
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
            try:
                positions.append(self.consumer.position([tp])[0].offset)
            except KafkaError as error:
                logging.error("Could not get position for topic-partition %s: %s", tp, error)
        return positions

    def close(self):
        """
        Close the consumer.
        """
        self.consumer.close()
