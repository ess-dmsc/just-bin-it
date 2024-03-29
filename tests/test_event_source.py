import numpy as np
import pytest

from just_bin_it.endpoints.sources import EventSource, TooOldTimeRequestedException
from tests.doubles.consumer import (
    StubConsumer,
    StubConsumerRecord,
    get_fake_ev42_messages,
)


def compare_two_messages(sent, received):
    sent_timestamp, sent_offset, sent_event_data = sent
    rec_timestamp, rec_offset, rec_event_data = received
    if sent_timestamp != rec_timestamp:
        return False
    if sent_offset != rec_offset:
        return False

    if sent_event_data.source_name != rec_event_data[0]:
        return False
    if sent_event_data.pulse_time != rec_event_data[1]:
        return False
    if not np.array_equal(sent_event_data.time_of_flight, rec_event_data[2]):
        return False
    if not np.array_equal(sent_event_data.detector_id, rec_event_data[3]):
        return False

    return True


def serialise_messages(messages):
    result = []
    for ts, offset, event_data in messages:
        result.append(
            (
                StubConsumerRecord(
                    ts,
                    offset,
                    (
                        event_data.source_name,
                        event_data.pulse_time,
                        event_data.time_of_flight,
                        event_data.detector_id,
                        None,
                    ),
                )
            )
        )
    return result


class TestEventSourceSinglePartition:
    @classmethod
    def setup_class(cls):
        # Only need to create the messages once
        cls.messages = get_fake_ev42_messages(100)
        cls.serialised_messages = serialise_messages(cls.messages)

    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = StubConsumer(["broker"], ["topic"])
        self.consumer.add_messages(self.serialised_messages)
        self.event_source = EventSource(
            self.consumer, 0, deserialise_function=lambda x: x
        )

    def test_if_no_new_messages_then_no_data(self):
        consumer = StubConsumer(["broker"], ["topic"])
        event_source = EventSource(consumer, 0)

        data = event_source.get_new_data()
        assert len(data) == 0

    def test_if_x_new_messages_on_one_topic_then_data_has_x_items(self):
        data = self.event_source.get_new_data()

        assert len(data) == len(self.messages)
        for i, m in enumerate(self.messages):
            assert compare_two_messages(m, data[i])

    def test_given_exact_time_finds_start_message(self):
        _, _, expected_event_data = self.messages[45]
        self.event_source.start_time = expected_event_data.pulse_time

        self.event_source.seek_to_start_time()
        new_data = self.event_source.get_new_data()

        assert compare_two_messages(self.messages[45], new_data[0])

    def test_given_approximate_time_finds_start_pulse(self):
        _, _, expected_message = self.messages[45]
        self.event_source.start_time = expected_message.pulse_time - 5

        self.event_source.seek_to_start_time()
        new_data = self.event_source.get_new_data()

        assert compare_two_messages(self.messages[45], new_data[0])

    def test_given_too_old_time_then_throws(self):
        self.event_source.start_time = -1

        with pytest.raises(TooOldTimeRequestedException):
            self.event_source.seek_to_start_time()

    def test_given_time_more_recent_than_last_message_then_seeks_to_last_message(self):
        last_timestamp, last_offset, _ = self.messages[99]
        self.event_source.start_time = last_timestamp + 1000

        offset = self.event_source.seek_to_start_time()

        assert offset == [100]

    def test_query_for_exact_last_message_time_finds_correct_offset(self):
        expected_timestamp, expected_offset, expected_message = self.messages[95]
        self.event_source.start_time = expected_timestamp

        offset = self.event_source.seek_to_start_time()

        assert offset == [expected_offset]

    def test_query_for_inaccurate_last_message_time_finds_next_offset(self):
        expected_timestamp, expected_offset, expected_message = self.messages[95]
        # Go back a little, so it should find the expected message
        self.event_source.start_time = expected_timestamp - 5

        offset = self.event_source.seek_to_start_time()

        assert offset == [expected_offset]


class TestEventSourceMultiplePartitions:
    @classmethod
    def setup_class(cls):
        # Only need to create the messages once
        cls.messages = get_fake_ev42_messages(150, 3)
        cls.serialised_messages = serialise_messages(cls.messages)

    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = StubConsumer(["broker"], ["topic"], num_partitions=3)
        self.event_source = EventSource(
            self.consumer, 0, deserialise_function=lambda x: x
        )

        self.consumer.add_messages(self.serialised_messages[0::3], 0)
        self.consumer.add_messages(self.serialised_messages[1::3], 1)
        self.consumer.add_messages(self.serialised_messages[2::3], 2)

    def test_if_no_new_messages_then_no_data(self):
        consumer = StubConsumer(["broker"], ["topic"], num_partitions=3)
        event_source = EventSource(consumer, 0)

        data = event_source.get_new_data()
        assert len(data) == 0

    def test_if_x_new_messages_on_only_one_partition_then_data_has_x_items(self):
        consumer = StubConsumer(["broker"], ["topic"], num_partitions=3)
        event_source = EventSource(consumer, 0, deserialise_function=lambda x: x)
        messages = get_fake_ev42_messages(5)
        consumer.add_messages(serialise_messages(messages))

        data = event_source.get_new_data()

        assert len(data) == len(messages)
        for i, m in enumerate(messages):
            assert compare_two_messages(m, data[i])

    def test_if_x_new_messages_spread_across_partition_then_data_has_x_items(self):
        data = self.event_source.get_new_data()

        assert len(data) == len(self.messages)

    def test_given_exact_time_finds_start_of_newer_messages_across_all_partitions(self):
        _, _, expected_message = self.messages[45]
        self.event_source.start_time = expected_message.pulse_time

        self.event_source.seek_to_start_time()
        new_data = self.event_source.get_new_data()

        assert compare_two_messages(self.messages[45], new_data[0])

    def test_given_approximate_time_finds_start_pulse_across_all_partitions(self):
        _, _, expected_message = self.messages[45]
        self.event_source.start_time = expected_message.pulse_time - 5

        self.event_source.seek_to_start_time()
        new_data = self.event_source.get_new_data()

        assert compare_two_messages(self.messages[45], new_data[0])

    def test_given_time_more_recent_than_last_message_then_seeks_to_last_message_on_all_partitions(
        self,
    ):
        last_timestamp, _, _ = self.messages[149]
        msg_time = last_timestamp + 1000
        self.event_source.start_time = msg_time
        offsets = self.event_source.seek_to_start_time()

        assert offsets == [50, 50, 50]

    def test_query_for_exact_last_message_time_finds_correct_offset_for_all_partitions(
        self,
    ):
        expected_timestamp, _, _ = self.messages[149]
        self.event_source.start_time = expected_timestamp
        offsets = self.event_source.seek_to_start_time()

        assert offsets == [50, 50, 49]

    def test_query_for_inaccurate_last_message_time_finds_next_offset_for_all_partitions(
        self,
    ):
        expected_timestamp, _, _ = self.messages[149]
        # Go back a little, so it should find the expected message
        self.event_source.start_time = expected_timestamp - 5

        offsets = self.event_source.seek_to_start_time()

        assert offsets == [50, 50, 49]
