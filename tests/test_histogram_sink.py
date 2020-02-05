import pytest
from just_bin_it.endpoints.histogram_sink import HistogramSink
from just_bin_it.utilities.mock_producer import MockProducer, MockThrowsProducer


TEST_MESSAGE = "this is a message"
TEST_TOPIC = "topic1"
INFO = "info message"
TIMESTAMP = 1234567890


class TestHistogramSink:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.producer = MockProducer()
        self.sink = HistogramSink(self.producer, lambda x, y, z: (x, y, z))

    def test_if_no_producer_supplied_then_raises(self):
        with pytest.raises(Exception):
            HistogramSink(None)

    def test_sending_a_message_only_use_defaults_for_other_values(self):
        self.sink.send_histogram(TEST_TOPIC, TEST_MESSAGE)

        assert len(self.producer.messages) == 1
        assert self.producer.messages[0] == (TEST_TOPIC, (TEST_MESSAGE, 0, ""))

    def test_sending_a_message_with_info_adds_the_info_message(self):
        self.sink.send_histogram(TEST_TOPIC, TEST_MESSAGE, information=INFO)

        assert len(self.producer.messages) == 1
        assert self.producer.messages[0] == (TEST_TOPIC, (TEST_MESSAGE, 0, INFO))

    def test_sending_a_message_with_timestamp_adds_value_to_the_message(self):
        self.sink.send_histogram(TEST_TOPIC, TEST_MESSAGE, TIMESTAMP)

        assert len(self.producer.messages) == 1
        assert self.producer.messages[0] == (TEST_TOPIC, (TEST_MESSAGE, TIMESTAMP, ""))

    def test_failure_to_send_raises(self):
        with pytest.raises(Exception):
            sink = HistogramSink(MockThrowsProducer())
            sink.send_histogram(TEST_TOPIC, TEST_MESSAGE)
