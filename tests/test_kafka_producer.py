import logging
from unittest import mock

import pytest

from just_bin_it.endpoints import kafka_producer
from just_bin_it.endpoints.kafka_producer import DELIVERY_WAIT_S, Producer


class FakeKafkaProducer:
    def __init__(self, options):
        self.options = options
        self.produced = []
        self.flush_timeouts = []
        self.full = False

    def produce(self, topic, message, on_delivery=None):
        if self.full:
            raise BufferError("Local: Queue full")
        self.produced.append((topic, message, on_delivery))

    def flush(self, timeout=None):
        self.flush_timeouts.append(timeout)
        return len(self.produced)

    def poll(self, timeout):
        return 0

    def list_topics(self, topic=None, timeout=-1):
        self.listed = (topic, timeout)
        if topic == "unreachable":
            raise Exception("timed out")


class FailedMessage:
    def topic(self):
        return "hist_topic"


class TestProducer:
    @pytest.fixture(autouse=True)
    def prepare(self):
        with mock.patch.object(kafka_producer, "KafkaProducer", FakeKafkaProducer):
            self.producer = Producer(["broker"], {})

    def test_publishing_waits_a_limited_time_for_delivery(self):
        self.producer.publish_message("topic", b"data")

        assert self.producer.producer.flush_timeouts == [DELIVERY_WAIT_S]
        assert self.producer.producer.produced[0][:2] == ("topic", b"data")

    def test_queue_is_limited(self):
        assert self.producer.producer.options["queue.buffering.max.kbytes"] > 0

    def test_full_queue_drops_the_message_and_logs(self, caplog):
        self.producer.producer.full = True

        with caplog.at_level(logging.ERROR):
            self.producer.publish_message("topic", b"data")

        assert "queue is full" in caplog.text

    def test_delivery_errors_are_logged(self, caplog):
        self.producer.publish_message("topic", b"data")
        on_delivery = self.producer.producer.produced[0][2]

        with caplog.at_level(logging.ERROR):
            on_delivery(
                "Broker: Message batch larger than segment size", FailedMessage()
            )

        assert "hist_topic" in caplog.text
        assert "segment size" in caplog.text

    def test_flush_returns_undelivered_count(self):
        self.producer.publish_message("topic", b"data")

        assert self.producer.flush(3) == 1
        assert self.producer.producer.flush_timeouts[-1] == 3

    def test_prepare_topic_gets_its_metadata(self):
        self.producer.prepare_topic("hist_topic")

        assert self.producer.producer.listed[0] == "hist_topic"
        assert self.producer.producer.listed[1] > 0

    def test_prepare_topic_failure_is_only_logged(self, caplog):
        with caplog.at_level(logging.WARNING):
            self.producer.prepare_topic("unreachable")

        assert "unreachable" in caplog.text
