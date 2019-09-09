import pytest
from endpoints.sources import EventSource, TooOldTimeRequestedException
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
        for i, m in enumerate(messages):
            assert data[i] == m

    def test_given_exact_time_finds_start_message(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        expected_timestamp, expected_offset, expected_message = messages[45]
        start_time = expected_message["pulse_time"]

        self.event_source.seek_to_time(start_time)
        new_data = self.event_source.get_new_data()
        timestamp, offset, message = new_data[0]

        assert timestamp == expected_timestamp
        assert offset == expected_offset
        assert message == expected_message

    def test_given_approximate_time_finds_start_pulse(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        expected_timestamp, expected_offset, expected_message = messages[45]
        start_time = expected_message["pulse_time"] - 5

        self.event_source.seek_to_time(start_time)
        new_data = self.event_source.get_new_data()
        timestamp, offset, message = new_data[0]

        assert timestamp == expected_timestamp
        assert offset == expected_offset
        assert message == expected_message

    def test_given_too_old_time_then_throws(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        start_time = -1

        with pytest.raises(TooOldTimeRequestedException):
            self.event_source.seek_to_time(start_time)

    def test_given_time_more_recent_than_last_message_then_seeks_to_last_message(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        last_timestamp, last_offset, _ = messages[99]
        msg_time = last_timestamp + 1000

        offset = self.event_source.seek_to_time(msg_time)

        assert offset == last_offset

    def test_query_for_exact_end_time_finds_correct_offset(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        expected_timestamp, expected_offset, expected_message = messages[95]
        stop_time = expected_timestamp

        offset = self.event_source.offsets_for_time(stop_time)

        assert offset == expected_offset

    def test_query_for_inaccurate_end_time_finds_next_offset(self):
        messages = get_fake_event_messages(100)
        self.consumer.add_messages(messages)
        expected_timestamp, expected_offset, expected_message = messages[95]
        # Go back a little, so it should find the expected message
        request_time = expected_timestamp - 5

        offset = self.event_source.offsets_for_time(request_time)

        assert offset == expected_offset
