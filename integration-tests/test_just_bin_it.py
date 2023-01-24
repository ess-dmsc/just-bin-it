import copy
import json
import os
import random
import sys
import time

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import (
    SCHEMAS_TO_DESERIALISERS,
    get_schema,
    serialise_ev42,
)
from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.utilities import time_in_ns
from just_bin_it.utilities.fake_data_generation import generate_fake_data

TOF_RANGE = (0, 100_000_000)
DET_RANGE = (1, 512)
NUM_BINS = 50
BROKERS = ["localhost:9092"]
CMD_TOPIC = "hist_commands"
RESPONSE_TOPIC = "hist_responses"

CONFIG_CMD = {
    "cmd": "config",
    "input_schema": "ev44",
    "output_schema": "hs01",
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "data_brokers": BROKERS,
            "data_topics": ["your topic goes here"],
            "tof_range": TOF_RANGE,
            "det_range": DET_RANGE,
            "num_bins": NUM_BINS,
            "topic": "your topic goes here",
            "id": "some_id",
        }
    ],
}


STOP_CMD = {"cmd": "stop"}


def create_consumer(topic):
    consumer = KafkaConsumer(bootstrap_servers=BROKERS)
    topic_partitions = []

    partition_numbers = consumer.partitions_for_topic(topic)

    for pn in partition_numbers:
        topic_partitions.append(TopicPartition(topic, pn))

    consumer.assign(topic_partitions)
    # Move to end
    consumer.seek_to_end()
    return consumer, topic_partitions


def generate_data(msg_id, time_stamp, num_events):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_events)
    return serialise_ev42("integration test", msg_id, time_stamp, tofs, dets)


class TestJustBinIt:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": BROKERS[0], "api.version.request": True}
        admin_client = AdminClient(conf)
        uid = time_in_ns() // 1000
        self.hist_topic_name = f"hist_{uid}"
        self.data_topic_name = f"data_{uid}"
        hist_topic = NewTopic(self.hist_topic_name, 1, 1)
        data_topic = NewTopic(self.data_topic_name, 2, 1)
        admin_client.create_topics([hist_topic, data_topic])

        self.producer = KafkaProducer(bootstrap_servers=BROKERS)
        time.sleep(1)
        self.consumer, topic_partitions = create_consumer(self.hist_topic_name)
        # Only one partition for histogram topic
        self.topic_part = topic_partitions[0]
        self.initial_offset = self.consumer.position(self.topic_part)
        self.time_stamps = []
        self.num_events_per_msg = []

    def create_basic_config(self):
        config = copy.deepcopy(CONFIG_CMD)
        config["histograms"][0]["topic"] = self.hist_topic_name
        config["histograms"][0]["data_topics"] = [self.data_topic_name]
        return config

    def check_offsets_have_advanced(self):
        # Check that end offset has changed otherwise we could be looking at old test
        # data
        self.consumer.seek_to_end(self.topic_part)
        final_offset = self.consumer.position(self.topic_part)
        if final_offset <= self.initial_offset:
            raise Exception("No new data found on the topic - is just-bin-it running?")

    def send_message(self, topic, message, timestamp=None):
        if timestamp:
            self.producer.send(topic, message, timestamp_ms=timestamp)
        else:
            self.producer.send(topic, message)
        self.producer.flush()

    def generate_and_send_data(self, msg_id):
        time_stamp = time_in_ns()
        # Generate a random number of events so we can be sure the correct data matches
        # up at the end.
        num_events = random.randint(500, 1500)
        data = generate_data(msg_id, time_stamp, num_events)

        # Need timestamp in ms
        self.time_stamps.append(time_stamp // 1_000_000)
        self.num_events_per_msg.append(num_events)
        # Set the message timestamps explicitly so kafka latency effects are minimised.
        self.send_message(self.data_topic_name, data, self.time_stamps[~0])

    def get_hist_data_from_kafka(self):
        data = []
        # Move it to one from the end so we can read the final histogram
        self.consumer.seek_to_end(self.topic_part)
        end_pos = self.consumer.position(self.topic_part)
        self.consumer.seek(self.topic_part, end_pos - 1)

        while not data:
            data = self.consumer.poll(5)

        msg = data[self.topic_part][-1]
        schema = get_schema(msg.value)
        if schema in SCHEMAS_TO_DESERIALISERS:
            return SCHEMAS_TO_DESERIALISERS[schema](msg.value)

    def get_response_message_from_kafka(self):
        data = []
        consumer, topic_partitions = create_consumer(RESPONSE_TOPIC)
        topic_part = topic_partitions[0]
        # Move it to one from the end so we can read the final message
        consumer.seek_to_end(topic_part)
        end_pos = consumer.position(topic_part)
        consumer.seek(topic_part, end_pos - 1)

        while not data:
            data = consumer.poll(5)

        msg = data[topic_part][-1]
        return msg.value

    def ensure_topic_is_not_empty_on_startup(self):
        #  Put some data in it, so jbi doesn't complain about an empty topic
        for i in range(10):
            self.generate_and_send_data(i)
        time.sleep(1)

    def test_number_events_histogrammed_equals_number_events_generated_for_open_ended(
        self, just_bin_it
    ):
        # Configure just-bin-it
        config = self.create_basic_config()
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send fake data
        num_msgs = 10

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        total_events = sum(self.num_events_per_msg)

        time.sleep(5)

        self.check_offsets_have_advanced()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "COUNTING"

    @pytest.mark.flaky(reruns=5)
    def test_number_events_histogrammed_correspond_to_start_and_stop_times(
        self, just_bin_it
    ):
        # Send fake data
        num_msgs = 10

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        # Configure just-bin-it with start and stop times
        # Include only 5 messages in the interval
        start = self.time_stamps[3] - 1
        stop = self.time_stamps[7] + 1
        total_events = sum(self.num_events_per_msg[3:8])

        config = self.create_basic_config()
        config["start"] = start
        config["stop"] = stop
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(5)

        self.check_offsets_have_advanced()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "FINISHED"

    @pytest.mark.flaky(reruns=5)
    def test_counting_for_an_interval_gets_all_data_during_interval(self, just_bin_it):
        self.ensure_topic_is_not_empty_on_startup()

        # Config just-bin-it
        interval_length = 5
        config = self.create_basic_config()
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send fake data
        num_msgs = 12

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        time.sleep(interval_length * 3)

        self.check_offsets_have_advanced()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == total_events
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_with_no_data_exits_interval(self, just_bin_it):
        self.ensure_topic_is_not_empty_on_startup()

        # Config just-bin-it
        interval_length = 5
        config = self.create_basic_config()
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send no data, but wait for the interval to pass
        time.sleep(interval_length * 3)

        self.check_offsets_have_advanced()

        hist_data = self.get_hist_data_from_kafka()
        info = json.loads(hist_data["info"])

        assert hist_data["data"].sum() == 0
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_with_only_one_event_message_gets_data(
        self, just_bin_it
    ):
        self.ensure_topic_is_not_empty_on_startup()

        # Config just-bin-it
        interval_length = 5
        config = self.create_basic_config()
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send one fake data message
        self.generate_and_send_data(1)

        # Send no data, but wait for interval to pass
        time.sleep(interval_length * 3)

        self.check_offsets_have_advanced()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == total_events
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_data_after_empty_interval_is_ignored(
        self, just_bin_it
    ):
        self.ensure_topic_is_not_empty_on_startup()

        # Config just-bin-it
        interval_length = 5
        config = self.create_basic_config()
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send no data, but wait for interval to pass
        time.sleep(interval_length)

        # Send one fake data message just after the interval, but probably in the leeway
        self.generate_and_send_data(1)

        time.sleep(interval_length * 3)

        self.check_offsets_have_advanced()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == 0
        assert info["state"] == "FINISHED"

    def test_open_ended_counting_for_a_while_then_stop_command_triggers_finished(
        self, just_bin_it
    ):
        # Configure just-bin-it
        config = self.create_basic_config()
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send fake data
        num_msgs = 10

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        total_events = sum(self.num_events_per_msg)

        time.sleep(5)

        self.check_offsets_have_advanced()

        self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))
        time.sleep(1)

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "FINISHED"

    def test_supplying_msg_id_get_acknowledgement_response(self, just_bin_it):
        # Configure just-bin-it
        config = self.create_basic_config()
        config["msg_id"] = f"{time_in_ns() // 1000}"
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it some time before stopping it
        time.sleep(5)
        self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))
        time.sleep(1)

        msg = self.get_response_message_from_kafka()

        assert json.loads(msg)["msg_id"] == config["msg_id"]
        assert json.loads(msg)["response"] == "ACK"

    def test_supplying_msg_id_get_error_response(self, just_bin_it):
        # Configure just-bin-it
        config = self.create_basic_config()
        config["cmd"] = "not a valid command"
        config["msg_id"] = f"{time_in_ns() // 1000}"
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        time.sleep(2)

        msg = self.get_response_message_from_kafka()
        msg = json.loads(msg)

        assert msg["msg_id"] == config["msg_id"]
        assert msg["response"] == "ERR"
        assert "message" in msg

    @pytest.mark.flaky(reruns=5)
    def test_legacy_schemas(self, just_bin_it):
        self.ensure_topic_is_not_empty_on_startup()

        # Config just-bin-it
        interval_length = 5
        config = self.create_basic_config()
        config["interval"] = interval_length
        config["input_schema"] = "ev42"
        config["output_schema"] = "hs00"
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send fake data
        num_msgs = 12

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        time.sleep(interval_length * 3)

        self.check_offsets_have_advanced()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == total_events
        assert info["state"] == "FINISHED"
