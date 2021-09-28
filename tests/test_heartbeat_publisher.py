from os import getpid
from socket import gethostname

import pytest
from streaming_data_types import deserialise_x5f2

from just_bin_it.endpoints.heartbeat_publisher import HeartbeatPublisher
from just_bin_it.utilities import time_in_ns
from tests.doubles.producers import SpyProducer, StubProducerThatThrows

TEST_TOPIC = "topic1"


class TestHeartbeatPublisher:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.producer = SpyProducer()
        self.update_interval = 1000
        self.publisher = HeartbeatPublisher(
            self.producer, TEST_TOPIC, self.update_interval
        )

    def test_first_message_published_immediately(self):
        time_way_in_the_future = time_in_ns() * 10

        self.publisher.publish(time_way_in_the_future)

        assert len(self.producer.messages) == 1

    def test_after_first_message_do_not_publish_if_interval_has_not_passed(self):
        current_time_ms = time_in_ns() // 1_000_000
        # Ignore first message
        self.publisher.publish(current_time_ms)

        current_time_ms = (
            self.publisher.next_time_to_publish - self.update_interval // 10
        )
        self.publisher.publish(current_time_ms)

        assert len(self.producer.messages) == 1

    def test_message_contents_are_correct(self):
        current_time_ms = time_in_ns() // 1_000_000

        self.publisher.publish(current_time_ms)

        _, msg = self.producer.messages[0]
        msg = deserialise_x5f2(msg)
        assert msg.software_name == "just-bin-it"
        assert msg.host_name == gethostname()
        assert msg.process_id == getpid()
        assert msg.update_interval == self.update_interval
        assert msg.service_id == ""
        assert msg.software_version == ""
        assert msg.status_json == ""

    def test_after_first_message_publish_if_interval_has_passed(self):
        current_time_ms = time_in_ns() // 1_000_000
        # Ignore first message
        self.publisher.publish(current_time_ms)

        current_time_ms = self.publisher.next_time_to_publish
        self.publisher.publish(current_time_ms)

        assert len(self.producer.messages) == 2

    def test_silently_fails_if_cannot_producer_fails(self):
        publisher = HeartbeatPublisher(
            StubProducerThatThrows(), TEST_TOPIC, self.update_interval
        )

        publisher.publish(1_234_567_890)
