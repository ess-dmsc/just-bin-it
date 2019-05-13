import pytest
from endpoints.sources import ConfigSource
from tests.mock_consumer import MockConsumer


CONFIG_JSON_1 = """
{
  "cmd": "config",
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"type": "hist1d", "tof_range": [0, 100000000], "num_bins": 50, "topic": "topic1", "source": "source1"},
    {"type": "hist2d", "det_range": [10, 1234], "num_bins": 100, "topic": "topic2", "source": "source2"}
  ]
}
"""

CONFIG_JSON_2 = """
{
  "cmd": "config",
  "data_brokers": ["differenthost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"type": "hist1d", "tof_range": [0, 100000000], "num_bins": 50, "topic": "topic1"}
  ]
}
"""

CONFIG_JSON_3 = """
{
  "cmd": "config",
  "data_brokers": ["differenthost:9092"],
  "data_topics": ["TEST_events"],
  "time_range": [
  "histograms": [
    {"type": "hist1d", "tof_range": [0, 100000000], "num_bins": 50, "topic": "topic1"}
  ]
}
"""

RESTART_JSON = """
{
  "cmd": "restart"
}
"""

INVALID_JSON = '{ "malformed": 123]'


class TestConfigSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception):
            ConfigSource(None)

    def test_received_configuration_converted_correctly(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [CONFIG_JSON_1])
        src = ConfigSource(mock_consumer)

        config = src.get_new_data()[-1]

        assert config["cmd"] == "config"
        assert len(config["data_brokers"]) == 1
        assert config["data_brokers"][0] == "localhost:9092"
        assert len(config["data_topics"]) == 1
        assert config["data_topics"][0] == "TEST_events"
        assert len(config["histograms"]) == 2
        assert config["histograms"][0]["type"] == "hist1d"
        assert config["histograms"][0]["tof_range"] == [0, 100000000]
        assert config["histograms"][0]["num_bins"] == 50
        assert config["histograms"][1]["type"] == "hist2d"
        assert config["histograms"][1]["det_range"] == [10, 1234]
        assert config["histograms"][1]["num_bins"] == 100
        assert config["histograms"][0]["topic"] == "topic1"
        assert config["histograms"][1]["topic"] == "topic2"
        assert config["histograms"][0]["source"] == "source1"
        assert config["histograms"][1]["source"] == "source2"

    def test_received_restart_message_converted_correctly(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [RESTART_JSON])
        src = ConfigSource(mock_consumer)

        message = src.get_new_data()[-1]

        assert message["cmd"] == "restart"

    def test_no_messages_returns_none(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [])
        src = ConfigSource(mock_consumer)

        assert len(src.get_new_data()) == 0

    def test_if_multiple_new_messages_gets_the_most_recent_one(self):
        mock_consumer = MockConsumer(
            ["broker1"], ["topic1"], [CONFIG_JSON_1, CONFIG_JSON_1, CONFIG_JSON_2]
        )
        src = ConfigSource(mock_consumer)

        config = src.get_new_data()[-1]

        assert config["data_brokers"][0] == "differenthost:9092"

    def test_malformed_message_is_ignored(self):
        mock_consumer = MockConsumer(["broker1"], ["topic1"], [INVALID_JSON])
        src = ConfigSource(mock_consumer)

        assert len(src.get_new_data()) == 0

    def test_if_multiple_new_messages_gets_the_most_recent_valid_one(self):
        mock_consumer = MockConsumer(
            ["broker1"], ["topic1"], [CONFIG_JSON_1, CONFIG_JSON_2, INVALID_JSON]
        )
        src = ConfigSource(mock_consumer)

        config = src.get_new_data()[-1]

        assert config["data_brokers"][0] == "differenthost:9092"
