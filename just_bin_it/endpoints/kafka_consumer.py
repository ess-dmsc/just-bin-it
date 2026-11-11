import logging
import uuid

from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import KafkaException as KafkaError
from confluent_kafka import TopicPartition

from just_bin_it.exceptions import KafkaException


class Consumer:
    """
    Consumes the messages from Kafka.

    This contains the least amount of logic so as to make mocking and testing
    easier.

    Note: Can only handle one topic.
    """

    def __init__(self, brokers, topics, security_config, assign_to_end=True):
        """
        Constructor.

        :param brokers: The names of the brokers to connect to.
        :param topics: The names of the data topics.
        :param security_config: The security config for Kafka
        :param assign_to_end: Whether to assign partitions to their latest offsets.
        """
        self.topic_partitions = []
        self._assigned = False
        self._next_offsets = {}
        try:
            self.consumer = self._create_consumer(brokers, security_config)
            self._assign_topics(topics, assign_to_end)
        except KafkaError as error:
            raise KafkaException(error)

    def _create_consumer(self, brokers, security_config):
        options = {
            "bootstrap.servers": ",".join(brokers),
            "group.id": uuid.uuid4(),
            # So at_end() notices idle partitions quickly
            "fetch.wait.max.ms": 100,
        }
        return KafkaConsumer({**options, **security_config})

    def _assign_topics(self, topics, assign_to_end):
        # Only use the first topic
        topic = topics[0]

        metadata = self.consumer.list_topics(topic)
        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None or topic_metadata.error is not None:
            raise KafkaException(f"Requested topic {topic} not available")

        partition_numbers = topic_metadata.partitions.keys()

        for pn in partition_numbers:
            self.topic_partitions.append(TopicPartition(topic, pn))

        if assign_to_end:
            # Seek to the end of each partition
            for tp in self.topic_partitions:
                high_watermark = self.consumer.get_watermark_offsets(tp, cached=False)[
                    1
                ]
                tp.offset = high_watermark
                self._next_offsets[tp.partition] = high_watermark

            self.consumer.assign(self.topic_partitions)
            self._assigned = True

    def get_new_messages(self):
        """
        Get any new messages.

        :return: The list containing the messages.
        """
        data = []
        while True:
            messages = self.consumer.consume(timeout=0.005)
            if not messages:
                break
            for message in messages:
                if message.error():
                    logging.warning("Kafka consumer error: %s", message.error())
                    continue
                self._next_offsets[message.partition()] = message.offset() + 1
                data.append(message)

            for tp in self.topic_partitions:
                logging.debug(
                    "%s - current position: %s",
                    tp.topic,
                    self.consumer.position([tp])[0].offset,
                )
        return data

    def at_end(self):
        """
        Whether all partitions have been read up to their high watermark, as
        of the last fetch (no request to the broker).

        :return: True if all partitions have been read to their end.
        """
        if not self._assigned:
            return False
        try:
            for tp in self.topic_partitions:
                _, high = self.consumer.get_watermark_offsets(tp, cached=True)
                if high < 0 or self._next_offsets.get(tp.partition, -1) < high:
                    return False
        except KafkaError as error:
            logging.debug("Could not get cached watermarks: %s", error)
            return False
        return True

    def offset_for_time(self, start_time: int):
        """
        Find the offset to the position corresponding to the supplied time.

        :param start_time: Time to seek in microseconds.
        :return: The offset number.
        """
        partitions = [
            TopicPartition(tp.topic, tp.partition, start_time)
            for tp in self.topic_partitions
        ]
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
        for tp, offset in zip(self.topic_partitions, offsets):
            tp.offset = offset
            self._next_offsets[tp.partition] = offset

        if self._assigned:
            for tp in self.topic_partitions:
                self.consumer.seek(tp)
        else:
            self.consumer.assign(self.topic_partitions)
            self._assigned = True

    def get_offset_range(self):
        """
        Get the lowest and highest offsets for the topic.

        :return: Tuple of lowest and highest offsets.
        """
        offset_ranges = []
        for tp in self.topic_partitions:
            try:
                (low, high) = self.consumer.get_watermark_offsets(tp)
                offset_ranges.append((low, high))
            except KafkaError as error:
                logging.error(
                    "Could not get watermark offsets for topic-partition %s: %s",
                    tp,
                    error,
                )
        return offset_ranges

    def get_positions(self):
        """
        Get the position of the consumer for each partition.

        :return: List of positions.
        """
        positions = []
        for tp in self.topic_partitions:
            logging.error(self.consumer.position([tp]))
            try:
                positions.append(self.consumer.position([tp])[0].offset)
            except KafkaError as error:
                logging.error(
                    "Could not get position for topic-partition %s: %s", tp, error
                )
        return positions

    def close(self):
        """
        Close the consumer.
        """
        self.consumer.close()
