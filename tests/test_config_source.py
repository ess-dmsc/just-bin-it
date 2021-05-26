import pytest

from just_bin_it.endpoints.sources import ConfigSource
from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE
from tests.doubles.consumer import StubConsumer

CONFIG_BASIC_1 = """
{
  "cmd": "config",
  "data_brokers": ["localhost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {
        "type": "hist1d",
        "tof_range": [0, 100000000],
        "num_bins": 50,
        "topic": "topic1",
        "source": "source1",
        "id": "my-hist-1"
    },
    {
        "type": "hist2d",
        "det_range": [10, 1234],
        "num_bins": 100,
        "topic": "topic2",
        "source": "source2",
        "id": "my-hist-2"
    }
  ]
}
"""

CONFIG_BASIC_2 = """
{
  "cmd": "config",
  "data_brokers": ["differenthost:9092"],
  "data_topics": ["TEST_events"],
  "histograms": [
    {"type": "hist1d", "tof_range": [0, 100000000], "num_bins": 50, "topic": "topic1"}
  ]
}
"""

CONFIG_START_AND_STOP = """
{
  "cmd": "config",
  "data_brokers": ["differenthost:9092"],
  "data_topics": ["TEST_events"],
  "start": 1558596988319002000,
  "stop": 1558597215933670000,
  "histograms": [
    {"type": "hist1d", "tof_range": [0, 100000000], "num_bins": 50, "topic": "topic1"}
  ]
}
"""

CONFIG_INTERVAL = """
{
  "cmd": "config",
  "data_brokers": ["differenthost:9092"],
  "data_topics": ["TEST_events"],
  "interval": 5000,
  "histograms": [
    {"type": "hist1d", "tof_range": [0, 100000000], "num_bins": 50, "topic": "topic1"}
  ]
}
"""

RESET_CMD = """
{
  "cmd": "reset_counts"
}
"""

INVALID_JSON = '{ "malformed": 123]'


class TestConfigSource:
    def test_if_no_consumer_supplied_then_raises(self):
        with pytest.raises(Exception):
            ConfigSource(None)

    def test_received_configuration_converted_correctly(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages([(0, 0, CONFIG_BASIC_1)])
        src = ConfigSource(consumer)

        _, _, config = src.get_new_data()[-1]

        assert config["cmd"] == "config"
        assert len(config["data_brokers"]) == 1
        assert config["data_brokers"][0] == "localhost:9092"
        assert len(config["data_topics"]) == 1
        assert config["data_topics"][0] == "TEST_events"
        assert len(config["histograms"]) == 2

        assert config["histograms"][0]["type"] == TOF_1D_TYPE
        assert config["histograms"][0]["tof_range"] == [0, 100000000]
        assert config["histograms"][0]["num_bins"] == 50
        assert config["histograms"][0]["topic"] == "topic1"
        assert config["histograms"][0]["source"] == "source1"
        assert config["histograms"][0]["id"] == "my-hist-1"

        assert config["histograms"][1]["type"] == TOF_2D_TYPE
        assert config["histograms"][1]["det_range"] == [10, 1234]
        assert config["histograms"][1]["num_bins"] == 100
        assert config["histograms"][1]["topic"] == "topic2"
        assert config["histograms"][1]["source"] == "source2"
        assert config["histograms"][1]["id"] == "my-hist-2"

    def test_received_restart_message_converted_correctly(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages([(0, 0, RESET_CMD)])
        src = ConfigSource(consumer)

        _, _, message = src.get_new_data()[-1]

        assert message["cmd"] == "reset_counts"

    def test_no_messages_returns_none(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages([])
        src = ConfigSource(consumer)

        assert len(src.get_new_data()) == 0

    def test_if_multiple_new_messages_gets_the_most_recent_one(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages(
            [(0, 0, CONFIG_BASIC_1), (1, 1, CONFIG_BASIC_1), (2, 2, CONFIG_BASIC_2)]
        )
        src = ConfigSource(consumer)

        _, _, config = src.get_new_data()[-1]

        assert config["data_brokers"][0] == "differenthost:9092"

    def test_malformed_message_is_ignored(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages([(0, 0, INVALID_JSON)])
        src = ConfigSource(consumer)

        assert len(src.get_new_data()) == 0

    def test_if_multiple_new_messages_gets_the_most_recent_valid_one(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages(
            [(0, 0, CONFIG_BASIC_1), (1, 1, CONFIG_BASIC_2), (2, 2, INVALID_JSON)]
        )
        src = ConfigSource(consumer)

        _, _, config = src.get_new_data()[-1]

        assert config["data_brokers"][0] == "differenthost:9092"

    def test_start_and_stop_time_converted_correctly(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages([(0, 0, CONFIG_START_AND_STOP)])
        src = ConfigSource(consumer)

        _, _, config = src.get_new_data()[-1]

        assert config["start"] == 1558596988319002000
        assert config["stop"] == 1558597215933670000

    def test_interval_converted_correctly(self):
        consumer = StubConsumer(["broker1"], ["topic1"])
        consumer.add_messages([(0, 0, CONFIG_INTERVAL)])
        src = ConfigSource(consumer)

        _, _, config = src.get_new_data()[-1]

        assert config["interval"] == 5000
