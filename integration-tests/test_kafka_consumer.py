import os
import sys
import threading
import time

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from integration_settings import BROKERS

from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.exceptions import KafkaException
from just_bin_it.utilities import time_in_ns

POLL_INTERVAL_S = 0.05
KAFKA_TIMEOUT_S = 15


def create_topics(admin_client, topics):
    futures = admin_client.create_topics(topics)
    for future in futures.values():
        future.result(timeout=KAFKA_TIMEOUT_S)

    # Wait until every partition has a leader, otherwise early requests fail.
    deadline = time.monotonic() + KAFKA_TIMEOUT_S
    names = [topic.topic for topic in topics]
    while time.monotonic() < deadline:
        metadata = admin_client.list_topics(timeout=KAFKA_TIMEOUT_S).topics
        if all(
            name in metadata
            and metadata[name].partitions
            and all(p.leader >= 0 for p in metadata[name].partitions.values())
            for name in names
        ):
            return
        time.sleep(POLL_INTERVAL_S)
    raise AssertionError(f"Topics {names} have no leaders")


def wait_for_messages(consumer, number_messages, timeout=KAFKA_TIMEOUT_S):
    deadline = time.monotonic() + timeout
    messages = []

    while time.monotonic() < deadline:
        messages.extend(consumer.get_new_messages())
        if len(messages) == number_messages:
            return messages
        if len(messages) > number_messages:
            raise AssertionError(
                f"Received {len(messages)} Kafka messages, "
                f"expected {number_messages}"
            )
        time.sleep(POLL_INTERVAL_S)

    raise AssertionError(
        f"Timed out waiting for Kafka messages: "
        f"received {len(messages)} of {number_messages}"
    )


class TestKafkaConsumer:
    """
    Tests to check that our Consumer class and the Kafka module work together
    the way we think they do.

    Note: on the multiple partition tests it is possible that a partition could
    be empty which would cause the test to fail. The number of messages is quite
    high, so it should be unlikely
    """

    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": BROKERS[0]}
        admin_client = AdminClient(conf)
        uid = time_in_ns() // 1000
        self.one_partition_topic_name = f"one_{uid}"
        self.three_partition_topic_name = f"three_{uid}"
        one_partition_topic = NewTopic(self.one_partition_topic_name, 1, 1)
        three_partition_topic = NewTopic(self.three_partition_topic_name, 3, 1)
        create_topics(admin_client, [one_partition_topic, three_partition_topic])

        self.producer = Producer(conf)

        self.num_messages = 50

    def put_messages_in(self, topic_name, number_messages, partitions=None):
        for i in range(number_messages):
            msg = f"msg-{i}"
            if partitions is None:
                self.producer.produce(topic_name, msg.encode())
            else:
                self.producer.produce(
                    topic_name, msg.encode(), partition=i % partitions
                )
        self.producer.flush()

    def create_consumer(self, topic):
        # A new topic can briefly report "not leader for partition".
        deadline = time.monotonic() + KAFKA_TIMEOUT_S
        while True:
            try:
                return Consumer(BROKERS, [topic], {})
            except KafkaException:
                if time.monotonic() > deadline:
                    raise
                time.sleep(POLL_INTERVAL_S)

    def test_all_data_retrieved_when_one_partition(self):
        consumer = self.create_consumer(self.one_partition_topic_name)
        self.put_messages_in(self.one_partition_topic_name, self.num_messages)
        # Move to beginning
        consumer.seek_by_offsets([0])

        data = wait_for_messages(consumer, self.num_messages)

        assert isinstance(data, list)
        # Total messages
        assert len(data) == self.num_messages
        # Check the types
        assert isinstance(data[0].offset(), int)
        assert isinstance(data[0].timestamp()[1], int)
        assert isinstance(data[0].value(), bytes)

    def test_all_data_retrieved_when_three_partitions(self):
        self.put_messages_in(
            self.three_partition_topic_name, self.num_messages, partitions=3
        )
        consumer = self.create_consumer(self.three_partition_topic_name)
        # Move to beginning
        consumer.seek_by_offsets([0, 0, 0])

        data = wait_for_messages(consumer, self.num_messages)

        assert isinstance(data, list)
        # Total messages across all partitions
        assert len(data) == self.num_messages
        # Check the types
        assert isinstance(data[0].offset(), int)
        assert isinstance(data[0].timestamp()[1], int)
        assert isinstance(data[0].value(), bytes)

    def wait_for_end(self, consumer, timeout=KAFKA_TIMEOUT_S):
        deadline = time.monotonic() + timeout
        messages = []
        while time.monotonic() < deadline:
            messages.extend(consumer.get_new_messages())
            if consumer.at_end():
                return messages
            time.sleep(POLL_INTERVAL_S)
        raise AssertionError("Timed out waiting for the end of the partitions")

    def test_end_of_an_empty_topic_is_reached(self):
        consumer = self.create_consumer(self.three_partition_topic_name)

        assert self.wait_for_end(consumer) == []

    def test_end_is_reached_only_after_all_partitions_are_read(self):
        # Partition 2 stays empty.
        self.put_messages_in(self.three_partition_topic_name, 10, partitions=2)
        consumer = self.create_consumer(self.three_partition_topic_name)
        consumer.seek_by_offsets([0, 0, 0])

        assert not consumer.at_end()
        assert len(self.wait_for_end(consumer)) == 10

    def test_end_is_left_when_new_data_arrives_until_it_is_read(self):
        consumer = self.create_consumer(self.one_partition_topic_name)
        self.wait_for_end(consumer)

        self.put_messages_in(self.one_partition_topic_name, 1)
        # Let the fetch bring the new end of the partition, without reading.
        time.sleep(0.5)

        assert not consumer.at_end()
        assert len(self.wait_for_end(consumer)) == 1

    def test_end_is_reached_while_data_keeps_arriving(self):
        # Must be at the end even though new data keeps coming
        consumer = self.create_consumer(self.three_partition_topic_name)
        self.wait_for_end(consumer)
        stop = threading.Event()

        def produce():
            i = 0
            while not stop.is_set():
                self.producer.produce(
                    self.three_partition_topic_name, b"x", partition=i % 3
                )
                self.producer.poll(0)
                i += 1
                time.sleep(1 / 14)

        thread = threading.Thread(target=produce)
        thread.start()
        try:
            checks = at_end = 0
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                consumer.get_new_messages()
                checks += 1
                at_end += consumer.at_end()
                time.sleep(0.005)
        finally:
            stop.set()
            thread.join()
            self.producer.flush()

        assert at_end > checks / 2

    def test_get_offsets_for_time_after_last_message(self):
        self.put_messages_in(
            self.three_partition_topic_name, self.num_messages, partitions=3
        )
        current_time = time_in_ns() // 1_000_000
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.offset_for_time(current_time)

        # For times after the last message, the offsets should be -1
        assert offsets == [-1, -1, -1]

    def test_get_offsets_for_time_before_first_message(self):
        current_time = time_in_ns() // 1_000_000
        self.put_messages_in(
            self.three_partition_topic_name, self.num_messages, partitions=3
        )
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.offset_for_time(current_time)

        # For times before the first message, the offsets should be 0
        assert offsets == [0, 0, 0]

    def test_get_offset_ranges(self):
        self.put_messages_in(
            self.three_partition_topic_name, self.num_messages, partitions=3
        )
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.get_offset_range()

        # Start offsets should all be zero
        assert [start for start, _ in offsets] == [0, 0, 0]
        # End offsets should sum to the number of messages
        assert sum([end for _, end in offsets]) == self.num_messages

    def test_seek_and_get_position(self):
        self.put_messages_in(
            self.three_partition_topic_name, self.num_messages, partitions=3
        )
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.get_offset_range()
        # Pick somewhere in the middle
        new_offsets = [(end - start) // 2 for start, end in offsets]

        consumer.seek_by_offsets(new_offsets)

        expected_num_messages = self.num_messages - sum(new_offsets)
        num_messages_since_offset = len(
            wait_for_messages(consumer, expected_num_messages)
        )

        assert num_messages_since_offset == expected_num_messages


class TestKafkaTools:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": BROKERS[0]}
        admin_client = AdminClient(conf)
        uid = time_in_ns() // 1000
        self.one_partition_topic_name = f"one_{uid}"
        self.three_partition_topic_name = f"three_{uid}"
        one_partition_topic = NewTopic(self.one_partition_topic_name, 1, 1)
        three_partition_topic = NewTopic(self.three_partition_topic_name, 3, 1)
        create_topics(admin_client, [one_partition_topic, three_partition_topic])

        self.producer = Producer(conf)

    def test_checking_for_non_existent_broker_is_not_valid(self):
        assert not are_kafka_settings_valid(
            ["invalid_broker"], [self.one_partition_topic_name], {}
        )

    def test_checking_for_non_existent_topic_is_not_valid(self):
        assert not are_kafka_settings_valid(BROKERS, ["not_a_real_topic"], {})

    def test_checking_for_valid_broker_and_topic_is_valid(self):
        assert are_kafka_settings_valid(BROKERS, [self.one_partition_topic_name], {})

    def test_checking_for_valid_broker_and_multiple_topics_is_valid(self):
        assert are_kafka_settings_valid(
            BROKERS,
            [self.one_partition_topic_name, self.three_partition_topic_name],
            {},
        )
