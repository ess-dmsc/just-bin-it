import copy
import pytest
from just_bin_it.config_handler import ConfigHandler


CONFIG_SINGLE = {
    "cmd": "config",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["junk_data_2"],
    "start": 1571831082207,
    "stop": 1571831125940,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "det_range": [0, 512],
            "num_bins": 50,
            "topic": "hist-topic",
            "id": "some_id",
        }
    ],
}


class TestConfigHandler:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_single_histogram_then_all_config_settings_added_correctly(self):
        handler = ConfigHandler()

        result = handler.parse(CONFIG_SINGLE)

        assert len(result) == 1
        assert result[0]["start"] == CONFIG_SINGLE["start"]
        assert result[0]["stop"] == CONFIG_SINGLE["stop"]
        assert result[0]["data_brokers"] == CONFIG_SINGLE["data_brokers"]
        assert result[0]["data_topics"] == CONFIG_SINGLE["data_topics"]
        assert result[0]["type"] == CONFIG_SINGLE["histograms"][0]["type"]
        assert result[0]["tof_range"] == CONFIG_SINGLE["histograms"][0]["tof_range"]
        assert result[0]["det_range"] == CONFIG_SINGLE["histograms"][0]["det_range"]
        assert result[0]["num_bins"] == CONFIG_SINGLE["histograms"][0]["num_bins"]
        assert result[0]["topic"] == CONFIG_SINGLE["histograms"][0]["topic"]
        assert result[0]["id"] == CONFIG_SINGLE["histograms"][0]["id"]

    def test_if_single_histogram_with_no_start_and_no_stop_are_none(self):
        handler = ConfigHandler()
        config = copy.deepcopy(CONFIG_SINGLE)
        del config["start"]
        del config["stop"]

        result = handler.parse(config)

        assert len(result) == 1
        assert result[0]["start"] == CONFIG_SINGLE["start"]

    # def test_if_single_histogram_with_interval_then_interval_added_correctly(self):
    #     handler = ConfigHandler()
    #     config = copy.deepcopy(CONFIG_SINGLE)
    #     del config["start"]
    #     del config["stop"]
    #     config["interval"] = 5
    #
    #     result = handler.parse(config)
    #
    #     assert len(result) == 1
    #     assert result[0]["start"] == CONFIG_SINGLE["start"]
