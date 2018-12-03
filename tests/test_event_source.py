import pytest
from unittest.mock import patch
from event_source import EventSource


TEST_MESSAGE=b"this is a byte message"


class MockConsumerRecord:
    def __init__(self, value):
        self.value = value


class MockConsumer:
    def __init__(
        self, brokers, topics, num_messages, message=TEST_MESSAGE
    ):
        self.brokers = brokers
        self.topics = topics
        self.num_messages = num_messages
        self.message = message

    def get_new_messages(self):
        # From Kafka we get a dictionary of topics which contains a list of
        # consumer records which we want 'value' from.
        # Recreate the structure here to match that.
        data = {}

        for t in self.topics:
            records = []
            for i in range(self.num_messages):
                records.append(MockConsumerRecord(self.message))
            data[t] = records
        return data


class TestEventSource(object):
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception, message="Expecting Exception from Constructor"):
            event_src = EventSource(None)

    def test_if_no_new_messages_then_no_data(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], 0)
        es = EventSource(mock_consumer)
        data = es.get_data()
        assert 0 == len(data)

    @patch('event_source.deserialise', return_value=TEST_MESSAGE)
    def test_if_five_new_messages_on_one_topic_then_data_has_five_items(self, mock_method):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], 5)
        es = EventSource(mock_consumer)
        data = es.get_data()
        assert 5 == len(data)
        assert TEST_MESSAGE == data[0]


