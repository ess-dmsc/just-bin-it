import pytest
from unittest.mock import patch
from endpoints.config_source import EventSource
from tests.mock_consumer import MockConsumer


TEST_MESSAGE = b"this is a byte message"
INVALID_FB = b"this is an invalid fb message"


class TestEventSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception):
            EventSource(None)

    def test_if_no_new_messages_then_no_data(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [])
        es = EventSource(mock_consumer)
        data = es.get_new_data()
        assert len(data) == 0

    @patch("endpoints.config_source.deserialise_ev42", return_value=TEST_MESSAGE)
    def test_if_five_new_messages_on_one_topic_then_data_has_five_items(
        self, mock_method
    ):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [TEST_MESSAGE] * 5)
        es = EventSource(mock_consumer)
        data = es.get_new_data()
        assert len(data) == 5
        assert data[0] == TEST_MESSAGE

    def test_deserialising_invalid_fb_does_not_throw(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [INVALID_FB])
        es = EventSource(mock_consumer)

        es.get_new_data()
