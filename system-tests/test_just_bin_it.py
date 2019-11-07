import copy
import json
import os
import random
import sys
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import serialise_ev42, deserialise_hs00
from just_bin_it.utilities.fake_data_generation import generate_fake_data

TOF_RANGE = (0, 100_000_000)
DET_RANGE = (1, 512)
NUM_BINS = 50
BROKERS = ["localhost:9092"]
DATA_TOPIC = "event_data"
HIST_TOPIC = "hist_topic"
CMD_TOPIC = "hist_commands"

CONFIG_JSON = {
    "cmd": "config",
    "data_brokers": BROKERS,
    "data_topics": [DATA_TOPIC],
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": TOF_RANGE,
            "det_range": DET_RANGE,
            "num_bins": NUM_BINS,
            "topic": HIST_TOPIC,
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
        self.producer = KafkaProducer(bootstrap_servers=BROKERS)
        self.consumer, self.topic_part = create_consumer(HIST_TOPIC)
        self.initial_offset = self.consumer.position(self.topic_part)
        self.time_stamps = []
        self.num_events_per_msg = []

    def check_offsets(self):
        # Check that end offset has changed otherwise we could be looking at old test
        # data
        self.consumer.seek_to_end(self.topic_part)
        final_offset = self.consumer.position(self.topic_part)
        if final_offset <= self.initial_offset:
            raise Exception("No new data found on the topic - is just-bin-it running?")

        # Move it back one so we can read the final histogram
        self.consumer.seek(self.topic_part, final_offset - 1)

    def send_message(self, topic, message):
        self.producer.send(topic, message)
        self.producer.flush()

    def generate_and_send_data(self, msg_id):
        time_stamp = time.time_ns()
        # Generate a random number of events so we can be sure the correct data matches
        # up at the end.
        num_events = random.randint(500, 1500)
        data = generate_data(msg_id, time_stamp, num_events)
        self.send_message(DATA_TOPIC, data)

        # Need timestamp in ms
        self.time_stamps.append(time_stamp // 1_000_000)
        self.num_events_per_msg.append(num_events)

    def get_hist_data_from_kafka(self):
        data = []
        while not data:
            data = self.consumer.poll(5)

        msg = data[self.topic_part][-1]
        return deserialise_hs00(msg.value)

    def test_number_events_histogrammed_equals_number_events_generated_for_open_ended(
        self
    ):
        # Configure just-bin-it
        self.send_message(CMD_TOPIC, bytes(json.dumps(CONFIG_JSON), "utf-8"))

        time.sleep(2)

        # Send fake data
        num_msgs = 10

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        total_events = sum(self.num_events_per_msg)

        # Check that end offset has changed otherwise we could be looking at old test
        # data
        self.check_offsets()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "COUNTING"

    def test_number_events_histogrammed_correspond_to_start_and_stop_times(self):
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

        config = copy.deepcopy(CONFIG_JSON)
        config["start"] = start
        config["stop"] = stop
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        time.sleep(2)

        # Check that end offset has changed otherwise we could be looking at old test data.
        self.check_offsets()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "FINISHED"

    def test_counting_for_an_interval_gets_all_data_during_interval(self):
        # Config just-bin-it
        interval_length = 5
        config = copy.deepcopy(CONFIG_JSON)
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Send fake data
        num_msgs = 12

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)
            time.sleep(0.5)

        time.sleep(2)

        # Check that end offset has changed otherwise we could be looking at old test data.
        self.check_offsets()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == total_events
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_with_no_data_exits_interval(self):
        # Config just-bin-it
        interval_length = 5
        config = copy.deepcopy(CONFIG_JSON)
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Send no data, but wait for the interval to pass
        time.sleep(interval_length * 2)

        # Check that end offset has changed otherwise we could be looking at old test
        # data.
        self.check_offsets()

        hist_data = self.get_hist_data_from_kafka()
        info = json.loads(hist_data["info"])

        assert hist_data["data"].sum() == 0
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_with_only_one_event_messge_gets_data(self):
        # Config just-bin-it
        interval_length = 5
        config = copy.deepcopy(CONFIG_JSON)
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send one fake data message
        self.generate_and_send_data(1)

        # Send no data, but wait for interval to pass
        time.sleep(interval_length * 2)

        # Check that end offset has changed otherwise we could be looking at old test data.
        self.check_offsets()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == total_events
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_data_after_empty_interval_is_ignored(self):
        # Config just-bin-it
        interval_length = 5
        config = copy.deepcopy(CONFIG_JSON)
        config["interval"] = interval_length
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        # Give it time to start counting
        time.sleep(1)

        # Send no data, but wait for interval to pass
        time.sleep(interval_length)

        # Send one fake data message just after the interval, but probably in the leeway
        self.generate_and_send_data(1)

        time.sleep(1)

        # Check that end offset has changed otherwise we could be looking at old test data.
        self.check_offsets()

        # Get histogram data
        hist_data = self.get_hist_data_from_kafka()

        info = json.loads(hist_data["info"])
        total_events = 0
        for ts, num in zip(self.time_stamps, self.num_events_per_msg):
            if info["start"] <= ts <= info["stop"]:
                total_events += num

        assert hist_data["data"].sum() == 0
        assert info["state"] == "FINISHED"
