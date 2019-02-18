import pytest
from unittest.mock import patch
from endpoints.config_source import HistogramSource
from tests.mock_consumer import MockConsumer


TEST_MESSAGE = b"this is a byte message"


class TestHistogramSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception, message="Expecting Exception from Constructor"):
            HistogramSource(None)

    def test_if_no_new_messages_then_no_data(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [])
        es = HistogramSource(mock_consumer)
        data = es.get_new_data()
        assert len(data) == 0

    @patch("endpoints.config_source.deserialise_hs00", return_value=TEST_MESSAGE)
    def test_if_five_new_messages_on_one_topic_then_data_has_five_items(
        self, mock_method
    ):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [TEST_MESSAGE] * 5)
        es = HistogramSource(mock_consumer)
        data = es.get_new_data()
        assert len(data) == 5
        assert data[0] == TEST_MESSAGE
