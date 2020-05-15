import os
import sys
from kafka import KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.utilities import time_in_ns
from test_just_bin_it import create_consumer


BROKERS = ["localhost:9092"]


class TestKafkaModuleContract:
    """Tests to check the Kafka module works the way we think it does"""

    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": BROKERS[0], "api.version.request": True}
        admin_client = AdminClient(conf)
        uid = time_in_ns() // 1000
        self.one_partition_topic_name = f"one_{uid}"
        self.three_partition_topic_name = f"three_{uid}"
        one_partition_topic = NewTopic(self.one_partition_topic_name, 1, 1)
        three_partition_topic = NewTopic(self.three_partition_topic_name, 3, 1)
        admin_client.create_topics([one_partition_topic, three_partition_topic])

        self.producer = KafkaProducer(bootstrap_servers=BROKERS)

    def put_messages_in(self, topic_name, number_messages):
        # Put messages in
        for i in range(number_messages):
            msg = f"msg-{i}"
            self.producer.send(topic_name, msg.encode())
        self.producer.flush()

    def test_all_data_retrieved_when_one_partition(self):
        self.put_messages_in(self.one_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.one_partition_topic_name)
        consumer.seek_to_beginning()

        data = {}
        while not data:
            data = consumer.poll(5)

        assert isinstance(data, dict)
        # One partition
        assert len(data) == 1
        assert topic_partitions[0] in data.keys()
        # Total messages
        assert len(data[topic_partitions[0]]) == 10
        # Check the types
        assert isinstance(data[topic_partitions[0]][0].offset, int)
        assert isinstance(data[topic_partitions[0]][0].timestamp, int)
        assert isinstance(data[topic_partitions[0]][0].value, bytes)

    def test_all_data_retrieved_when_three_partitions(self):
        self.put_messages_in(self.three_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.three_partition_topic_name)
        consumer.seek_to_beginning()

        data = {}
        while not data:
            data = consumer.poll(5)

        assert isinstance(data, dict)
        # Three partitions
        assert len(data) == 3
        for tp in topic_partitions:
            assert tp in data.keys()
        # Total messages across all partitions
        assert sum([len(data[tp]) for tp in topic_partitions]) == 10
        # Check the types
        assert isinstance(data[topic_partitions[0]][0].offset, int)
        assert isinstance(data[topic_partitions[0]][0].timestamp, int)
        assert isinstance(data[topic_partitions[0]][0].value, bytes)

    def test_get_offsets_for_time_after_last_message(self):
        self.put_messages_in(self.three_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.three_partition_topic_name)
        current_time = time_in_ns() // 1_000_000
        partitions_to_time = {tp: current_time for tp in topic_partitions}
        offsets = consumer.offsets_for_times(partitions_to_time)

        assert isinstance(offsets, dict)
        for tp in topic_partitions:
            # For times after the last message, the offset should be None
            assert offsets[tp] is None

    def test_get_offsets_for_time_before_first_message(self):
        current_time = time_in_ns() // 1_000_000

        self.put_messages_in(self.three_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.three_partition_topic_name)
        partitions_to_time = {tp: current_time for tp in topic_partitions}
        offsets = consumer.offsets_for_times(partitions_to_time)

        assert isinstance(offsets, dict)
        for tp in topic_partitions:
            # For times before the first message, the offset should be 0
            assert offsets[tp].offset == 0
            assert offsets[tp].timestamp >= current_time

    def test_get_beginning_offsets(self):
        self.put_messages_in(self.three_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.three_partition_topic_name)
        begin_offsets = consumer.beginning_offsets(topic_partitions)

        assert isinstance(begin_offsets, dict)
        for tp in topic_partitions:
            assert begin_offsets[tp] == 0

    def test_get_end_offsets(self):
        self.put_messages_in(self.three_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.three_partition_topic_name)
        end_offsets = consumer.end_offsets(topic_partitions)

        assert isinstance(end_offsets, dict)
        # Offsets should sum to the number of messages
        assert sum([end_offsets[tp] for tp in topic_partitions]) == 10

    def test_seek_and_get_position(self):
        self.put_messages_in(self.three_partition_topic_name, 10)

        consumer, topic_partitions = create_consumer(self.three_partition_topic_name)
        begin_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        # Just look at one partition
        tp = topic_partitions[0]
        begin = begin_offsets[tp]
        end = end_offsets[tp]

        # Pick somewhere in the middle
        middle = (end - begin) // 2

        consumer.seek(tp, middle)

        assert consumer.position(tp) == middle
