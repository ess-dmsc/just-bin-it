import pytest
from endpoints.sources import EventSource
from tests.mock_consumer import MockConsumer, get_fake_event_messages


class TestEventSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = MockConsumer(["broker"], ["topic"])
        self.event_source = EventSource(self.consumer, lambda x: x)

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception):
            EventSource(None)

    def test_if_no_new_messages_then_no_data(self):
        data = self.event_source.get_new_data()
        assert len(data) == 0

    def test_if_x_new_messages_on_one_topic_then_data_has_x_items(self):
        messages = get_fake_event_messages(5)
        self.consumer.add_messages(messages)

        data = self.event_source.get_new_data()

        assert len(data) == len(messages)
        for i in range(len(messages)):
            assert data[i] == messages[i]

    def test_given_exact_time_finds_start_pulse(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        msg = messages[45]
        start_time = msg["pulse_time"]

        self.event_source.seek_to_pulse_time(start_time)
        data = self.event_source.get_new_data()

        assert data[0]["pulse_time"] == msg["pulse_time"]

    def test_given_approximate_time_finds_start_pulse(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        msg = messages[45]
        start_time = msg["pulse_time"] - 5

        self.event_source.seek_to_pulse_time(start_time)
        data = self.event_source.get_new_data()

        assert data[0]["pulse_time"] == msg["pulse_time"]

    def test_given_too_old_time_then_throws(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        start_time = -1

        with pytest.raises(Exception):
            self.event_source.seek_to_pulse_time(start_time)
