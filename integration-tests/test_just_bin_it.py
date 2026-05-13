import copy
import json
import os
import random
import sys
import time
import uuid

import numpy as np
import pytest
from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import (
    SCHEMAS_TO_DESERIALISERS,
    get_schema,
    serialise_da00,
    serialise_ev44,
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
POLL_INTERVAL_S = 0.05
KAFKA_TIMEOUT_S = 15


def create_topics(admin_client, topics):
    futures = admin_client.create_topics(topics)
    for future in futures.values():
        future.result(timeout=KAFKA_TIMEOUT_S)


def deserialise_message(message):
    schema = get_schema(message)
    if schema in SCHEMAS_TO_DESERIALISERS:
        return SCHEMAS_TO_DESERIALISERS[schema](message)
    raise AssertionError(f"Unexpected schema {schema}")


def create_consumer(topic):
    consumer_conf = {
        "bootstrap.servers": ",".join(BROKERS),
        "group.id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(consumer_conf)
    topic_partitions = []

    metadata = consumer.list_topics(topic)
    partition_numbers = [p.id for p in metadata.topics[topic].partitions.values()]

    for pn in partition_numbers:
        partition = TopicPartition(topic, pn)
        # Make sure consumer is at end of the partition(s)
        partition.offset = OFFSET_END
        topic_partitions.append(partition)

    consumer.assign(topic_partitions)
    return consumer, topic_partitions


def generate_data(msg_id, time_stamp, num_events):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_events)
    return serialise_ev44("integration test", msg_id, time_stamp, tofs, dets)


def generate_da00_data(time_stamp, num_events):
    counts = np.zeros(NUM_BINS, dtype=np.int64)
    for _ in range(num_events):
        counts[random.randrange(NUM_BINS)] += 1
    edges = np.linspace(TOF_RANGE[0], TOF_RANGE[1], NUM_BINS + 1, dtype=np.int64)
    return serialise_da00(
        "integration test",
        time_stamp,
        [
            {"name": "signal", "data": counts, "axes": ["frame_time"]},
            {
                "name": "frame_time",
                "data": edges,
                "axes": ["frame_time"],
                "unit": "ns",
            },
        ],
    )


class TestJustBinIt:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": ",".join(BROKERS)}
        admin_client = AdminClient(conf)
        uid = time_in_ns() // 1000
        self.hist_topic_name = f"hist_{uid}"
        self.data_topic_name = f"data_{uid}"
        hist_topic = NewTopic(self.hist_topic_name, 1, 1)
        data_topic = NewTopic(self.data_topic_name, 2, 1)
        create_topics(admin_client, [hist_topic, data_topic])

        self.producer = Producer(
            {"bootstrap.servers": ",".join(BROKERS), "message.max.bytes": 100_000_000}
        )

        self.consumer, topic_partitions = create_consumer(self.hist_topic_name)
        self.response_consumer, _ = create_consumer(RESPONSE_TOPIC)
        # Only one partition for histogram topic
        self.topic_part = topic_partitions[0]
        self.time_stamps = []
        self.num_events_per_msg = []

    def create_basic_config(self):
        config = copy.deepcopy(CONFIG_CMD)
        config["histograms"][0]["topic"] = self.hist_topic_name
        config["histograms"][0]["data_topics"] = [self.data_topic_name]
        return config

    def create_da00_config(self):
        config = self.create_basic_config()
        config["input_schema"] = "da00"
        del config["histograms"][0]["det_range"]
        return config

    def send_message(self, topic, message, timestamp=None):
        if timestamp:
            self.producer.produce(topic, message, timestamp=timestamp)
        else:
            self.producer.produce(topic, message)
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

    def generate_and_send_da00_data(self):
        time_stamp = time_in_ns()
        # Generate a random number of counts so we can be sure the correct data matches
        # up at the end.
        num_events = random.randint(500, 1500)
        data = generate_da00_data(time_stamp, num_events)

        # Need timestamp in ms
        self.time_stamps.append(time_stamp // 1_000_000)
        self.num_events_per_msg.append(num_events)
        # Set the message timestamps explicitly so kafka latency effects are minimised.
        self.send_message(self.data_topic_name, data, self.time_stamps[~0])

    def wait_for_histogram(self, expected_sum, expected_state, timeout=KAFKA_TIMEOUT_S):
        deadline = time.monotonic() + timeout
        last_hist_data = None
        last_hist_info = None

        while time.monotonic() < deadline:
            msg = self.consumer.poll(POLL_INTERVAL_S)
            if msg is None:
                continue
            if msg.error():
                raise AssertionError(msg.error())

            last_hist_data = deserialise_message(msg.value())
            last_hist_info = json.loads(last_hist_data["info"])
            if (
                last_hist_data["data"].sum() == expected_sum
                and last_hist_info["state"] == expected_state
            ):
                return last_hist_data

        raise AssertionError(
            f"Timed out waiting for histogram sum={expected_sum}, "
            f"state={expected_state}. Last message was sum="
            f"{last_hist_data['data'].sum() if last_hist_data else None}, "
            f"info={last_hist_info}"
        )

    def wait_for_response(self, msg_id, expected_response, timeout=KAFKA_TIMEOUT_S):
        deadline = time.monotonic() + timeout
        last_response = None

        while time.monotonic() < deadline:
            msg = self.response_consumer.poll(POLL_INTERVAL_S)
            if msg is None:
                continue
            if msg.error():
                raise AssertionError(msg.error())

            last_response = json.loads(msg.value())
            if (
                last_response.get("msg_id") == msg_id
                and last_response.get("response") == expected_response
            ):
                return last_response

        raise AssertionError(
            f"Timed out waiting for {expected_response} response to {msg_id}. "
            f"Last response was {last_response}"
        )

    def test_basic_operation(self, just_bin_it):
        # Configure just-bin-it
        config = self.create_basic_config()
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
        self.wait_for_histogram(0, "INITIALISED")

        # Send fake data
        num_msgs = 10

        for i in range(num_msgs):
            self.generate_and_send_data(i + 1)

        total_events = sum(self.num_events_per_msg)

        # Get histogram data
        hist_data = self.wait_for_histogram(total_events, "COUNTING")
        hist_info = json.loads(hist_data["info"])

        assert hist_data["data"].sum() == total_events
        assert hist_info["state"] == "COUNTING"
        assert hist_info["sum"] == total_events

        self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))

        # Get histogram data
        hist_data = self.wait_for_histogram(total_events, "FINISHED")

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "FINISHED"

    def test_basic_da00_operation(self, just_bin_it):
        # Configure just-bin-it
        config = self.create_da00_config()
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
        self.wait_for_histogram(0, "INITIALISED")

        # Send fake data
        num_msgs = 10

        for _ in range(num_msgs):
            self.generate_and_send_da00_data()

        total_events = sum(self.num_events_per_msg)

        # Get histogram data
        hist_data = self.wait_for_histogram(total_events, "COUNTING")
        hist_info = json.loads(hist_data["info"])

        assert hist_data["data"].sum() == total_events
        assert hist_info["state"] == "COUNTING"
        assert hist_info["sum"] == total_events

        self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))

        # Get histogram data
        hist_data = self.wait_for_histogram(total_events, "FINISHED")

        assert hist_data["data"].sum() == total_events
        assert json.loads(hist_data["info"])["state"] == "FINISHED"

    def test_supplying_msg_id_gets_acknowledgement_response(self, just_bin_it):
        # Configure just-bin-it
        config = self.create_basic_config()
        config["msg_id"] = f"{time_in_ns() // 1000}"
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        msg = self.wait_for_response(config["msg_id"], "ACK")

        assert msg["msg_id"] == config["msg_id"]
        assert msg["response"] == "ACK"

        stop_cmd = {"cmd": "stop", "msg_id": f"{time_in_ns() // 1000}"}
        self.send_message(CMD_TOPIC, bytes(json.dumps(stop_cmd), "utf-8"))
        self.wait_for_response(stop_cmd["msg_id"], "ACK")

    def test_supplying_msg_id_gets_error_response(self, just_bin_it):
        # Configure just-bin-it
        config = self.create_basic_config()
        config["cmd"] = "not a valid command"
        config["msg_id"] = f"{time_in_ns() // 1000}"
        self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))

        msg = self.wait_for_response(config["msg_id"], "ERR")

        assert msg["msg_id"] == config["msg_id"]
        assert msg["response"] == "ERR"
        assert "message" in msg
