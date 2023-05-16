import logging
import copy
import json
import os
import random
import sys
import time

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_END
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import (
    SCHEMAS_TO_DESERIALISERS,
    get_schema,
    serialise_ev42,
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


def create_consumer(topic):
    consumer_conf = {
        'bootstrap.servers': ','.join(BROKERS),
        'group.id': f'test-group{str(time.time_ns())[-6::]}',
        'auto.offset.reset': 'latest',
    }
    consumer = Consumer(consumer_conf)
    topic_partitions = []

    metadata = consumer.list_topics(topic)
    partition_numbers = [p.id for p in metadata.topics[topic].partitions.values()]

    for pn in partition_numbers:
        topic_partitions.append(TopicPartition(topic, pn))

    # Seek to the end of each partition
    for tp in topic_partitions:
        high_watermark = consumer.get_watermark_offsets(tp, timeout=10.0, cached=False)[1]
        tp.offset = high_watermark
        # self.consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))
    consumer.assign(topic_partitions)
    return consumer, topic_partitions


def generate_data(msg_id, time_stamp, num_events):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_events)
    return serialise_ev44("integration test", msg_id, time_stamp, tofs, dets)


def generate_data_ev42(msg_id, time_stamp, num_events):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_events)
    return serialise_ev42("integration test", msg_id, time_stamp, tofs, dets)


class TestJustBinIt:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Create unique topics for each test
        conf = {"bootstrap.servers": ','.join(BROKERS)}
        admin_client = AdminClient(conf)
        uid = time_in_ns() // 1000
        self.hist_topic_name = f"hist_{uid}"
        # self.hist_topic_name = "hist_123"
        self.data_topic_name = f"data_{uid}"
        # self.data_topic_name = "data_123"
        hist_topic = NewTopic(self.hist_topic_name, 1, 1)
        data_topic = NewTopic(self.data_topic_name, 2, 1)
        admin_client.create_topics([hist_topic, data_topic])

        self.producer = Producer(
            {'bootstrap.servers': ','.join(BROKERS), 'message.max.bytes': 100_000_000}
        )
        time.sleep(5)

        self.consumer, topic_partitions = create_consumer(self.hist_topic_name)
        # Only one partition for histogram topic
        self.topic_part = topic_partitions[0]
        self.initial_offset = topic_partitions[0].offset
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
        l, h = self.consumer.get_watermark_offsets(self.topic_part)

        self.consumer.seek(TopicPartition(self.topic_part.topic, self.topic_part.partition, h))

        logging.error(l)
        logging.error(h)

        tp = self.consumer.assignment()[0]
        final_offset = tp.offset
        # final_offset = self.consumer.position([self.topic_part])[0].offset

        logging.error(final_offset)
        logging.error(self.initial_offset)

        if h <= self.initial_offset:
            raise Exception("No new data found on the topic - is just-bin-it running?")

    def send_message(self, topic, message, timestamp=None):

        def delivery_callback(error, message):
            msg = ''
            msg += "delivery_callback. error={}. message={}".format(error, message)
            msg += "message.topic={}".format(message.topic())
            msg += "message.timestamp={}".format(message.timestamp())
            msg += "message.key={}".format(message.key())
            # msg += "message.value={}".format(message.value())
            msg += "message.partition={}".format(message.partition())
            msg += "message.offset={}".format(message.offset())
            logging.error(msg)

        if timestamp:
            self.producer.produce(topic, message, timestamp=timestamp, on_delivery=delivery_callback)
        else:
            self.producer.produce(topic, message, on_delivery=delivery_callback)
        self.producer.flush()




    def generate_and_send_data(self, msg_id, schema="ev44"):
        time_stamp = time_in_ns()
        # Generate a random number of events so we can be sure the correct data matches
        # up at the end.
        num_events = random.randint(500, 1500)
        if schema == "ev42":
            data = generate_data_ev42(msg_id, time_stamp, num_events)
        else:
            data = generate_data(msg_id, time_stamp, num_events)



        # Need timestamp in ms
        self.time_stamps.append(time_stamp // 1_000_000)
        self.num_events_per_msg.append(num_events)
        # Set the message timestamps explicitly so kafka latency effects are minimised.
        self.send_message(self.data_topic_name, data, self.time_stamps[~0])

    def get_hist_data_from_kafka(self):
        data = []
        # Move it to one from the end so we can read the final histogram
        # self.consumer.seek(TopicPartition(self.topic_part.topic, self.topic_part.partition, OFFSET_END))
        # end_pos = self.consumer.position(self.topic_part)
        _, high_wm = self.consumer.get_watermark_offsets(self.topic_part)
        last_highest = max(0, high_wm - 1)
        self.consumer.seek(TopicPartition(self.topic_part.topic, self.topic_part.partition, last_highest))

        while not data:
            msg = self.consumer.poll(0.005)
            if msg:
                data.append(msg)

        last_msg = data[-1]
        schema = get_schema(last_msg.value())
        if schema in SCHEMAS_TO_DESERIALISERS:
            return SCHEMAS_TO_DESERIALISERS[schema](last_msg.value())

    def get_response_message_from_kafka(self):
        data = []
        consumer, topic_partitions = create_consumer(RESPONSE_TOPIC)
        topic_part = topic_partitions[0]
        # Move it to one from the end so we can read the final message
        # consumer.seek(TopicPartition(topic_part.topic, topic_part.partition, OFFSET_END))
        # end_pos = consumer.position(topic_part)
        _, high_wm = consumer.get_watermark_offsets(topic_part)
        last_highest = max(0, high_wm - 1)
        consumer.seek(TopicPartition(topic_part.topic, topic_part.partition, last_highest))

        while not data:
            msg = consumer.poll(0.005)
            if msg:
                data.append(msg)

        last_msg = data[-1]
        return last_msg.value()

    def ensure_topic_is_not_empty_on_startup(self):
        #  Put some data in it, so jbi doesn't complain about an empty topic
        for i in range(10):
            self.generate_and_send_data(i)
        time.sleep(1)

    # def test_number_events_histogrammed_equals_number_events_generated_for_open_ended(
    #     self, just_bin_it
    # ):
    #     # Configure just-bin-it
    #     config = self.create_basic_config()
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it time to start counting
    #     time.sleep(1)
    #
    #     # Send fake data
    #     num_msgs = 10
    #
    #     for i in range(num_msgs):
    #         self.generate_and_send_data(i + 1)
    #         time.sleep(0.5)
    #
    #     total_events = sum(self.num_events_per_msg)
    #
    #     time.sleep(10)
    #
    #     self.check_offsets_have_advanced()
    #
    #     # Get histogram data
    #     hist_data = self.get_hist_data_from_kafka()
    #
    #     assert hist_data["data"].sum() == total_events
    #     assert json.loads(hist_data["info"])["state"] == "COUNTING"
    #
    # @pytest.mark.flaky(reruns=5)
    # def test_number_events_histogrammed_correspond_to_start_and_stop_times(
    #     self, just_bin_it
    # ):
    #     # Send fake data
    #     num_msgs = 10
    #
    #     for i in range(num_msgs):
    #         self.generate_and_send_data(i + 1)
    #         time.sleep(0.5)
    #
    #     # Configure just-bin-it with start and stop times
    #     # Include only 5 messages in the interval
    #     start = self.time_stamps[3] - 1
    #     stop = self.time_stamps[7] + 1
    #     total_events = sum(self.num_events_per_msg[3:8])
    #
    #     time.sleep(10)
    #
    #     config = self.create_basic_config()
    #     config["start"] = start
    #     config["stop"] = stop
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it time to start counting
    #     time.sleep(10)
    #
    #     self.check_offsets_have_advanced()
    #
    #     # Get histogram data
    #     hist_data = self.get_hist_data_from_kafka()
    #
    #     assert hist_data["data"].sum() == total_events
    #     assert json.loads(hist_data["info"])["state"] == "FINISHED"
    #
    # @pytest.mark.flaky(reruns=5)
    # def test_counting_for_an_interval_gets_all_data_during_interval(self, just_bin_it):
    #     self.ensure_topic_is_not_empty_on_startup()
    #
    #     # Config just-bin-it
    #     interval_length = 5
    #     config = self.create_basic_config()
    #     config["interval"] = interval_length
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it time to start counting
    #     time.sleep(1)
    #
    #     # Send fake data
    #     num_msgs = 12
    #
    #     for i in range(num_msgs):
    #         self.generate_and_send_data(i + 1)
    #         time.sleep(0.5)
    #
    #     time.sleep(interval_length * 3)
    #
    #     time.sleep(10)
    #
    #     self.check_offsets_have_advanced()
    #
    #     # Get histogram data
    #     hist_data = self.get_hist_data_from_kafka()
    #
    #     info = json.loads(hist_data["info"])
    #     total_events = 0
    #     for ts, num in zip(self.time_stamps, self.num_events_per_msg):
    #         if info["start"] <= ts <= info["stop"]:
    #             total_events += num
    #
    #     assert hist_data["data"].sum() == total_events
    #     assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_with_no_data_exits_interval(self, just_bin_it):  ## FAILED
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

        time.sleep(10)

        self.check_offsets_have_advanced()

        hist_data = self.get_hist_data_from_kafka()
        info = json.loads(hist_data["info"])

        assert hist_data["data"].sum() == 0
        assert info["state"] == "FINISHED"

    def test_counting_for_an_interval_with_only_one_event_message_gets_data(  ##FAILED
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

        time.sleep(10)

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

    # def test_counting_for_an_interval_data_after_empty_interval_is_ignored(
    #     self, just_bin_it
    # ):
    #     self.ensure_topic_is_not_empty_on_startup()
    #
    #     # Config just-bin-it
    #     interval_length = 5
    #     config = self.create_basic_config()
    #     config["interval"] = interval_length
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it time to start counting
    #     time.sleep(1)
    #
    #     # Send no data, but wait for interval to pass
    #     time.sleep(interval_length)
    #
    #     # Send one fake data message just after the interval, but probably in the leeway
    #     self.generate_and_send_data(1)
    #
    #     time.sleep(interval_length * 3)
    #
    #     self.check_offsets_have_advanced()
    #
    #     # Get histogram data
    #     hist_data = self.get_hist_data_from_kafka()
    #
    #     info = json.loads(hist_data["info"])
    #     total_events = 0
    #     for ts, num in zip(self.time_stamps, self.num_events_per_msg):
    #         if info["start"] <= ts <= info["stop"]:
    #             total_events += num
    #
    #     assert hist_data["data"].sum() == 0
    #     assert info["state"] == "FINISHED"
    #
    # def test_open_ended_counting_for_a_while_then_stop_command_triggers_finished(
    #     self, just_bin_it
    # ):
    #     # Configure just-bin-it
    #     config = self.create_basic_config()
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it time to start counting
    #     time.sleep(1)
    #
    #     # Send fake data
    #     num_msgs = 10
    #
    #     for i in range(num_msgs):
    #         self.generate_and_send_data(i + 1)
    #         time.sleep(0.5)
    #
    #     total_events = sum(self.num_events_per_msg)
    #
    #     time.sleep(10)
    #
    #     self.check_offsets_have_advanced()
    #
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))
    #     time.sleep(1)
    #
    #     # Get histogram data
    #     hist_data = self.get_hist_data_from_kafka()
    #
    #     assert hist_data["data"].sum() == total_events
    #     assert json.loads(hist_data["info"])["state"] == "FINISHED"
    #
    # def test_supplying_msg_id_get_acknowledgement_response(self, just_bin_it):
    #     # Configure just-bin-it
    #     config = self.create_basic_config()
    #     config["msg_id"] = f"{time_in_ns() // 1000}"
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it some time before stopping it
    #     time.sleep(5)
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))
    #     time.sleep(1)
    #
    #     time.sleep(10)
    #
    #     msg = self.get_response_message_from_kafka()
    #
    #     assert json.loads(msg)["msg_id"] == config["msg_id"]
    #     assert json.loads(msg)["response"] == "ACK"
    #
    # def test_supplying_msg_id_get_error_response(self, just_bin_it):
    #     # Configure just-bin-it
    #     config = self.create_basic_config()
    #     config["cmd"] = "not a valid command"
    #     config["msg_id"] = f"{time_in_ns() // 1000}"
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     time.sleep(2)
    #
    #     time.sleep(10)
    #
    #     msg = self.get_response_message_from_kafka()
    #     msg = json.loads(msg)
    #
    #     assert msg["msg_id"] == config["msg_id"]
    #     assert msg["response"] == "ERR"
    #     assert "message" in msg
    #
    # @pytest.mark.flaky(reruns=5)
    # def test_legacy_schemas(self, just_bin_it):
    #     self.ensure_topic_is_not_empty_on_startup()
    #
    #     # Config just-bin-it
    #     interval_length = 5
    #     config = self.create_basic_config()
    #     config["interval"] = interval_length
    #     config["input_schema"] = "ev42"
    #     config["output_schema"] = "hs00"
    #     self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
    #
    #     # Give it time to start counting
    #     time.sleep(1)
    #
    #     # Send fake data
    #     num_msgs = 12
    #
    #     for i in range(num_msgs):
    #         self.generate_and_send_data(i + 1, "ev42")
    #         time.sleep(0.5)
    #
    #     time.sleep(interval_length * 3)
    #
    #     time.sleep(10)
    #
    #     self.check_offsets_have_advanced()
    #
    #     # Get histogram data
    #     hist_data = self.get_hist_data_from_kafka()
    #
    #     info = json.loads(hist_data["info"])
    #     total_events = 0
    #     for ts, num in zip(self.time_stamps, self.num_events_per_msg):
    #         if info["start"] <= ts <= info["stop"]:
    #             total_events += num
    #
    #     assert hist_data["data"].sum() == total_events
    #     assert info["state"] == "FINISHED"
