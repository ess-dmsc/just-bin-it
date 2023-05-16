import logging
import time
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
        servers = ','.join(brokers)
        print(servers)
        return KafkaConsumer({"bootstrap.servers": ",".join(brokers), "group.id": f"mygroup{str(time.time_ns())[-6::]}"})

    def _assign_topics(self, topics):
        # Only use the first topic
        topic = topics[0]

        metadata = self.consumer.list_topics(timeout=0.01)
        available_topics = set(metadata.topics.keys())

        if topic not in available_topics:
            raise KafkaException(f"Requested topic {topic} not available")

        partition_numbers = metadata.topics[topic].partitions.keys()

        for pn in partition_numbers:
            self.topic_partitions.append(TopicPartition(topic, pn))


        # Seek to the end of each partition
        for tp in self.topic_partitions:
            high_watermark = self.consumer.get_watermark_offsets(tp, timeout=0.010, cached=False)[1]
            tp.offset = high_watermark
            # self.consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))
        self.consumer.assign(self.topic_partitions)

    def _get_new_messages(self):
        data = {}
        while True:
            messages = self.consumer.consume(timeout=0.005)
            # messages = self.consumer.poll(timeout=1)
            if not messages:
                break
            for message in messages:
                partition = message.partition()
                if partition not in data:
                    data[partition] = []
                data[partition].append(message)

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
        partitions = [TopicPartition(tp.topic, tp.partition, start_time) for tp in self.topic_partitions]
        offsets = self.consumer.offsets_for_times(partitions)
        result = []
        for tp in self.topic_partitions:
            offset_and_timestamp = offsets[offsets.index(tp)]
            result.append(offset_and_timestamp.offset)
        return result

    def seek_by_offsets(self, offsets):
        """
        Move to the specified offsets.

        :param offsets: The requested offsets.
        """
        self._seek_by_offsets(offsets)

    def _seek_by_offsets(self, offsets):
        for tp, offset in zip(self.topic_partitions, offsets):
            tp.offset = offset

        for tp in self.topic_partitions:
            self.consumer.seek(tp)


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
                (low, high) = self.consumer.get_watermark_offsets(tp, timeout=0.01)
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
            logging.error(self.consumer.position([tp]))
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
