import pytest
from endpoints.histogram_sink import HistogramSink
from tests.mock_producer import MockProducer, MockThrowsProducer


TEST_MESSAGE = "this is a message"
TEST_TOPIC = "topic1"
INFO = "info message"


class TestHistogramSink:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.producer = MockProducer()
        self.sink = HistogramSink(self.producer, lambda x, y: (x, y))

    def test_if_no_producer_supplied_then_raises(self):
        with pytest.raises(Exception):
            HistogramSink(None)

    def test_sending_a_message_sends_a_message(self):
        self.sink.send_histogram(TEST_TOPIC, TEST_MESSAGE)

        assert len(self.producer.messages) == 1
        assert self.producer.messages[0] == (TEST_TOPIC, (TEST_MESSAGE, ""))

    def test_sending_a_message_with_info_adds_the_info_message(self):
        self.sink.send_histogram(TEST_TOPIC, TEST_MESSAGE, INFO)

        assert len(self.producer.messages) == 1
        assert self.producer.messages[0] == (TEST_TOPIC, (TEST_MESSAGE, INFO))

    def test_failure_to_send_raises(self):
        with pytest.raises(Exception):
            sink = HistogramSink(MockThrowsProducer())
            sink.send_histogram(TEST_TOPIC, TEST_MESSAGE)
