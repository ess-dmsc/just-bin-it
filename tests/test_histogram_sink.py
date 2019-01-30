import pytest
from unittest.mock import patch
from histogram_sink import HistogramSink
from tests.mock_producer import MockProducer, MockThrowsProducer


TEST_MESSAGE = "this is a message"
TEST_TOPIC = "topic1"


class TestConfigSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_no_producer_supplied_then_raises(self):
        with pytest.raises(Exception, message="Expecting Exception from Constructor"):
            HistogramSink(None)

    @patch("histogram_sink.serialise_hs00", return_value=TEST_MESSAGE)
    def test_sending_a_message_sends_a_message(self, mock_method):
        producer = MockProducer()
        sink = HistogramSink(producer)

        sink.send_histogram(TEST_TOPIC, TEST_MESSAGE)

        assert 1 == len(producer.messages)
        assert TEST_TOPIC, TEST_MESSAGE == producer.messages[0]

    def test_failure_to_send_raises(self):
        with pytest.raises(Exception, message="Expecting Exception from sending"):
            sink = HistogramSink(MockThrowsProducer())
            sink.send_histogram(TEST_TOPIC, TEST_MESSAGE)
