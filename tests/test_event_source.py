import pytest
from just_bin_it.endpoints.sources import EventSource, TooOldTimeRequestedException
from tests.mock_consumer import MockConsumer, get_fake_event_messages


class TestEventSourceSinglePartition:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = MockConsumer(["broker"], ["topic"])
        self.messages = get_fake_event_messages(100)
        self.consumer.add_messages(self.messages)
        self.event_source = EventSource(self.consumer, lambda x: x)

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception):
            EventSource(None)

    def test_if_no_new_messages_then_no_data(self):
        consumer = MockConsumer(["broker"], ["topic"])
        event_source = EventSource(consumer, lambda x: x)

        data = event_source.get_new_data()
        assert len(data) == 0

    def test_if_x_new_messages_on_one_topic_then_data_has_x_items(self):
        data = self.event_source.get_new_data()

        assert len(data) == len(self.messages)
        for i, m in enumerate(self.messages):
            assert data[i] == m

    def test_given_exact_time_finds_start_message(self):
        expected_timestamp, expected_offset, expected_message = self.messages[45]
        start_time = expected_message["pulse_time"]

        self.event_source.seek_to_time(start_time)
        new_data = self.event_source.get_new_data()
        timestamp, offset, message = new_data[0]

        assert timestamp == expected_timestamp
        assert offset == expected_offset
        assert message == expected_message

    def test_given_approximate_time_finds_start_pulse(self):
        expected_timestamp, expected_offset, expected_message = self.messages[45]
        start_time = expected_message["pulse_time"] - 5

        self.event_source.seek_to_time(start_time)
        new_data = self.event_source.get_new_data()
        timestamp, offset, message = new_data[0]

        assert timestamp == expected_timestamp
        assert offset == expected_offset
        assert message == expected_message

    def test_given_too_old_time_then_throws(self):
        start_time = -1

        with pytest.raises(TooOldTimeRequestedException):
            self.event_source.seek_to_time(start_time)

    def test_given_time_more_recent_than_last_message_then_seeks_to_last_message(self):
        last_timestamp, last_offset, _ = self.messages[99]
        msg_time = last_timestamp + 1000

        offset = self.event_source.seek_to_time(msg_time)

        assert offset == [100]

    def test_query_for_exact_end_time_finds_correct_offset(self):
        expected_timestamp, expected_offset, expected_message = self.messages[95]
        stop_time = expected_timestamp

        offset = self.event_source.seek_to_time(stop_time)

        assert offset == [expected_offset]

    def test_query_for_inaccurate_end_time_finds_next_offset(self):
        expected_timestamp, expected_offset, expected_message = self.messages[95]
        # Go back a little, so it should find the expected message
        request_time = expected_timestamp - 5

        offset = self.event_source.seek_to_time(request_time)

        assert offset == [expected_offset]


class TestEventSourceMultiplePartitions:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = MockConsumer(["broker"], ["topic"], num_partitions=3)
        self.event_source = EventSource(self.consumer, lambda x: x)

        self.messages = get_fake_event_messages(150, 3)
        self.consumer.add_messages(self.messages[0::3], 0)
        self.consumer.add_messages(self.messages[1::3], 1)
        self.consumer.add_messages(self.messages[2::3], 2)

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception):
            EventSource(None)

    def test_if_no_new_messages_then_no_data(self):
        consumer = MockConsumer(["broker"], ["topic"], num_partitions=3)
        event_source = EventSource(consumer, lambda x: x)

        data = event_source.get_new_data()
        assert len(data) == 0

    def test_if_x_new_messages_on_only_one_partition_then_data_has_x_items(self):
        consumer = MockConsumer(["broker"], ["topic"], num_partitions=3)
        event_source = EventSource(consumer, lambda x: x)
        messages = get_fake_event_messages(5)
        consumer.add_messages(messages)

        data = event_source.get_new_data()

        assert len(data) == len(messages)
        for i, m in enumerate(messages):
            assert data[i] == m

    def test_if_x_new_messages_spread_across_partition_then_data_has_x_items(self):
        data = self.event_source.get_new_data()

        assert len(data) == len(self.messages)

    def test_given_exact_time_finds_start_of_newer_messages_across_all_partitions(self):
        expected_timestamp, expected_offset, expected_message = self.messages[45]
        start_time = expected_message["pulse_time"]

        self.event_source.seek_to_time(start_time)
        new_data = self.event_source.get_new_data()
        timestamp, offset, message = new_data[0]

        assert timestamp == expected_timestamp
        assert offset == expected_offset
        assert message == expected_message

    def test_given_approximate_time_finds_start_pulse_across_all_partitions(self):
        expected_timestamp, expected_offset, expected_message = self.messages[45]
        start_time = expected_message["pulse_time"] - 5

        self.event_source.seek_to_time(start_time)
        new_data = self.event_source.get_new_data()
        timestamp, offset, message = new_data[0]

        assert timestamp == expected_timestamp
        assert offset == expected_offset
        assert message == expected_message

    def test_given_time_more_recent_than_last_message_then_seeks_to_last_message_on_all_partitions(
        self
    ):
        last_timestamp, _, _ = self.messages[149]
        msg_time = last_timestamp + 1000

        offsets = self.event_source.seek_to_time(msg_time)

        assert offsets == [50, 50, 50]

    def test_query_for_exact_end_time_finds_correct_offset_for_all_partitions(self):
        expected_timestamp, _, _ = self.messages[149]
        stop_time = expected_timestamp

        offsets = self.event_source.seek_to_time(stop_time)

        assert offsets == [50, 50, 49]

    def test_query_for_inaccurate_end_time_finds_next_offset_for_all_partitions(self):
        expected_timestamp, _, _ = self.messages[149]
        # Go back a little, so it should find the expected message
        request_time = expected_timestamp - 5

        offsets = self.event_source.seek_to_time(request_time)

        assert offsets == [50, 50, 49]
