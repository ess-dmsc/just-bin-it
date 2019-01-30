import pytest
from config_source import ConfigSource
from tests.mock_consumer import MockConsumer


VALID_JSON_1 = """
{
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"num_dims": 1, "det_range": [0, 100000000], "num_bins": 50, "topic": "topic1"},
    {"num_dims": 2, "det_range": [10, 1234], "num_bins": 100, "topic": "topic2"}
  ]
}
"""

VALID_JSON_2 = """
{
  "data_brokers": ["differenthost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"num_dims": 1, "det_range": [0, 100000000], "num_bins": 50, "topic": "topic1"}
  ]
}
"""

INVALID_JSON = '{ "malformed": 123]'


class TestConfigSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception, message="Expecting Exception from Constructor"):
            ConfigSource(None)

    def test_received_configuration_converted_correctly(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [VALID_JSON_1])
        src = ConfigSource(mock_consumer)

        config = src.get_new_config()

        assert 1 == len(config["data_brokers"])
        assert "localhost:9092" == config["data_brokers"][0]
        assert 1 == len(config["data_topics"])
        assert "TEST_events" == config["data_topics"][0]
        assert 2 == len(config["histograms"])
        assert 1 == config["histograms"][0]["num_dims"]
        assert [0, 100000000] == config["histograms"][0]["det_range"]
        assert 50 == config["histograms"][0]["num_bins"]
        assert 2 == config["histograms"][1]["num_dims"]
        assert [10, 1234] == config["histograms"][1]["det_range"]
        assert 100 == config["histograms"][1]["num_bins"]
        assert "topic1" == config["histograms"][0]["topic"]
        assert "topic2" == config["histograms"][1]["topic"]

    def test_no_messages_returns_none(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [])
        src = ConfigSource(mock_consumer)

        assert src.get_new_config() is None

    def test_if_multiple_new_messages_gets_the_most_recent_one(self):
        mock_consumer = MockConsumer(
            ["broker1"], ["topic1"], [VALID_JSON_1, VALID_JSON_1, VALID_JSON_2]
        )
        src = ConfigSource(mock_consumer)

        config = src.get_new_config()

        assert "differenthost:9092" == config["data_brokers"][0]

    def test_malformed_message_is_ignored(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [INVALID_JSON])
        src = ConfigSource(mock_consumer)

        assert src.get_new_config() is None

    def test_if_multiple_new_messages_gets_the_most_recent_valid_one(self):
        mock_consumer = MockConsumer(
            ["broker1"], ["topic1"], [VALID_JSON_1, VALID_JSON_2, INVALID_JSON]
        )
        src = ConfigSource(mock_consumer)

        config = src.get_new_config()

        assert "differenthost:9092" == config["data_brokers"][0]
