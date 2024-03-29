import os
import sys
import time

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.utilities import time_in_ns

BROKERS = ["localhost:9092"]


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
        admin_client.create_topics([one_partition_topic, three_partition_topic])

        self.producer = Producer(conf)

        self.num_messages = 50
        # Ugly: give everything a chance to get going
        time.sleep(5)

    def put_messages_in(self, topic_name, number_messages):
        # Put messages in
        for i in range(number_messages):
            msg = f"msg-{i}"
            self.producer.produce(topic_name, msg.encode())
        self.producer.flush()

    def create_consumer(self, topic):
        consumer = Consumer(BROKERS, [topic], {})
        return consumer

    def test_all_data_retrieved_when_one_partition(self):
        consumer = self.create_consumer(self.one_partition_topic_name)
        self.put_messages_in(self.one_partition_topic_name, self.num_messages)
        # Move to beginning
        consumer.seek_by_offsets([0])

        data = {}
        while not data:
            data = consumer.get_new_messages()

        assert isinstance(data, list)
        # Total messages
        assert len(data) == self.num_messages
        # Check the types
        assert isinstance(data[0].offset(), int)
        assert isinstance(data[0].timestamp()[1], int)
        assert isinstance(data[0].value(), bytes)

    def test_all_data_retrieved_when_three_partitions(self):
        self.put_messages_in(self.three_partition_topic_name, self.num_messages)
        consumer = self.create_consumer(self.three_partition_topic_name)
        # Move to beginning
        consumer.seek_by_offsets([0, 0, 0])

        data = {}
        while not data:
            data = consumer.get_new_messages()

        assert isinstance(data, list)
        # Total messages across all partitions
        assert len(data) == self.num_messages
        # Check the types
        assert isinstance(data[0].offset(), int)
        assert isinstance(data[0].timestamp()[1], int)
        assert isinstance(data[0].value(), bytes)

    def test_get_offsets_for_time_after_last_message(self):
        self.put_messages_in(self.three_partition_topic_name, self.num_messages)
        current_time = time_in_ns() // 1_000_000
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.offset_for_time(current_time)

        # For times after the last message, the offsets should be -1
        assert offsets == [-1, -1, -1]

    def test_get_offsets_for_time_before_first_message(self):
        current_time = time_in_ns() // 1_000_000
        self.put_messages_in(self.three_partition_topic_name, self.num_messages)
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.offset_for_time(current_time)

        # For times before the first message, the offsets should be 0
        assert offsets == [0, 0, 0]

    def test_get_offset_ranges(self):
        self.put_messages_in(self.three_partition_topic_name, self.num_messages)
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.get_offset_range()

        # Start offsets should all be zero
        assert [start for start, _ in offsets] == [0, 0, 0]
        # End offsets should sum to the number of messages
        assert sum([end for _, end in offsets]) == self.num_messages

    def test_seek_and_get_position(self):
        self.put_messages_in(self.three_partition_topic_name, self.num_messages)
        consumer = self.create_consumer(self.three_partition_topic_name)

        offsets = consumer.get_offset_range()
        # Pick somewhere in the middle
        new_offsets = [(end - start) // 2 for start, end in offsets]

        consumer.seek_by_offsets(new_offsets)

        time.sleep(5)

        num_messages_since_offset = len(consumer.get_new_messages())

        assert num_messages_since_offset == self.num_messages - sum(new_offsets)


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
        admin_client.create_topics([one_partition_topic, three_partition_topic])

        self.producer = Producer(conf)

        time.sleep(5)

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
