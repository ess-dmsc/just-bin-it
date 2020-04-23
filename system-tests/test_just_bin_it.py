import copy
import json
import os
import random
import sys
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import serialise_ev42, deserialise_hs00
from just_bin_it.utilities import time_in_ns
from just_bin_it.utilities.fake_data_generation import generate_fake_data

TOF_RANGE = (0, 100_000_000)
DET_RANGE = (1, 512)
NUM_BINS = 50
BROKERS = ["localhost:9092"]
CMD_TOPIC = "hist_commands"

CONFIG_JSON = {
    "cmd": "config",
    "data_brokers": BROKERS,
    "data_topics": ["your topic goes here"],
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": TOF_RANGE,
            "det_range": DET_RANGE,
            "num_bins": NUM_BINS,
            "topic": "your topic goes here",
            "id": "some_id",
        }
    ],
}


def create_consumer(topic):
    consumer = KafkaConsumer(bootstrap_servers=BROKERS)
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    # Move to end
    consumer.seek_to_end(tp)
    return consumer, tp


def generate_data(msg_id, time_stamp, num_events):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_events)
    return serialise_ev42("system test", msg_id, time_stamp, tofs, dets)


class TestJustBinIt:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": BROKERS[0], "api.version.request": True}
        admin_client = AdminClient(conf)
        uid = time_in_ns()
        self.hist_topic_name = f"hist_{uid}"
        self.data_topic_name = f"data_{uid}"
        hist_topic = NewTopic(self.hist_topic_name, 1, 1)
        data_topic = NewTopic(self.data_topic_name, 1, 1)
        admin_client.create_topics([hist_topic, data_topic])

        self.producer = KafkaProducer(bootstrap_servers=BROKERS)
        self.consumer, self.topic_part = create_consumer(self.hist_topic_name)
        self.initial_offset = self.consumer.position(self.topic_part)
        self.time_stamps = []
        self.num_events_per_msg = []

    def create_basic_config(self):
        config = copy.deepcopy(CONFIG_JSON)
        config["data_topics"] = [self.data_topic_name]
        config["histograms"][0]["topic"] = self.hist_topic_name
        return config

    def check_offsets_have_advanced(self):
        # Check that end offset has changed otherwise we could be looking at old test
        # data
        self.consumer.seek_to_end(self.topic_part)
        final_offset = self.consumer.position(self.topic_part)
        if final_offset <= self.initial_offset:
            raise Exception("No new data found on the topic - is just-bin-it running?")

    def send_message(self, topic, message):
        self.producer.send(topic, message)
        self.producer.flush()

    def generate_and_send_data(self, msg_id):
        time_stamp = time_in_ns()
        # Generate a random number of events so we can be sure the correct data matches
        # up at the end.
        num_events = random.randint(500, 1500)
        data = generate_data(msg_id, time_stamp, num_events)
        self.send_message(self.data_topic_name, data)

        # Need timestamp in ms
        self.time_stamps.append(time_stamp // 1_000_000)
        self.num_events_per_msg.append(num_events)

    def get_hist_data_from_kafka(self):
        data = []
        # Move it to one from the end so we can read the final histogram
        self.consumer.seek_to_end(self.topic_part)
        end_pos = self.consumer.position(self.topic_part)
        self.consumer.seek(self.topic_part, end_pos - 1)

        while not data:
            data = self.consumer.poll(5)

        msg = data[self.topic_part][-1]
        return deserialise_hs00(msg.value)

    def ensure_topic_is_not_empty_on_startup(self):
        #  Put some data in it, so jbi doesn't complain about an empty topic
        self.generate_and_send_data(0)

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
        start = self.time_stamps[3]
        stop = self.time_stamps[8]
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
        time.sleep(interval_length * 4)

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
