import pytest

from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram_factory import parse_config

CONFIG_FULL = {
    "cmd": "config",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["junk_data_2"],
    "start": 1571831082207,
    "stop": 1571831125940,
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": TOF_1D_TYPE,
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
    ],
}


class TestConfigParserOldStyle:
    """
    Tests specific to the old style configuration messages.
    Can be deleted when the old-style syntax is retired.
    """

    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_histograms_then_all_config_settings_added_correctly(self):
        start, stop, hists = parse_config(CONFIG_FULL)

        assert len(hists) == 2
        assert start == CONFIG_FULL["start"]
        assert stop == CONFIG_FULL["stop"]

        for i, h in enumerate(hists):
            assert h["data_brokers"] == CONFIG_FULL["data_brokers"]
            assert h["data_topics"] == CONFIG_FULL["data_topics"]
            assert h["type"] == CONFIG_FULL["histograms"][i]["type"]
            assert h["tof_range"] == CONFIG_FULL["histograms"][i]["tof_range"]
            assert h["det_range"] == CONFIG_FULL["histograms"][i]["det_range"]
            assert h["num_bins"] == CONFIG_FULL["histograms"][i]["num_bins"]
            assert h["topic"] == CONFIG_FULL["histograms"][i]["topic"]
            assert h["id"] == CONFIG_FULL["histograms"][i]["id"]
