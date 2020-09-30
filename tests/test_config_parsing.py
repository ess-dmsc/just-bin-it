import copy

import pytest

from just_bin_it.histograms.histogram_factory import parse_config

CONFIG_FULL = {
    "cmd": "config",
    "start": 1571831082207,
    "stop": 1571831125940,
    "histograms": [
        {
            "type": "hist1d",
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": "hist1d",
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
    ],
}

CONFIG_INTERVAL = {
    "cmd": "config",
    "interval": 5,
    "histograms": [
        {
            "type": "hist1d",
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": "hist1d",
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
    ],
}

CONFIG_NO_DET_RANGE = {
    "cmd": "config",
    "interval": 5,
    "histograms": [
        {
            "type": "hist1d",
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        }
    ],
}


class TestConfigParser:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_if_histograms_then_all_config_settings_added_correctly(self):
        start, stop, hists = parse_config(CONFIG_FULL)

        assert len(hists) == 2
        assert start == CONFIG_FULL["start"]
        assert stop == CONFIG_FULL["stop"]

        for i, h in enumerate(hists):
            assert h["data_brokers"] == CONFIG_FULL["histograms"][i]["data_brokers"]
            assert h["data_topics"] == CONFIG_FULL["histograms"][i]["data_topics"]
            assert h["type"] == CONFIG_FULL["histograms"][i]["type"]
            assert h["tof_range"] == CONFIG_FULL["histograms"][i]["tof_range"]
            assert h["det_range"] == CONFIG_FULL["histograms"][i]["det_range"]
            assert h["num_bins"] == CONFIG_FULL["histograms"][i]["num_bins"]
            assert h["topic"] == CONFIG_FULL["histograms"][i]["topic"]
            assert h["id"] == CONFIG_FULL["histograms"][i]["id"]

    def test_raises_if_no_data_brokers_supplied_for_a_hist(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"][0]["data_brokers"]

        with pytest.raises(Exception):
            parse_config(config)

    def test_raises_if_no_data_topics_supplied_for_a_hist(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"][0]["data_topics"]

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_no_start_and_stop_then_they_are_not_set(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["start"]
        del config["stop"]

        start, stop, _ = parse_config(config)

        assert start is None
        assert stop is None

    def test_if_interval_defined_then_start_and_stop_are_initialised(self):
        current_time = 123456
        start, stop, _ = parse_config(CONFIG_INTERVAL, current_time)

        assert start == current_time
        assert stop == current_time + CONFIG_INTERVAL["interval"] * 1000

    def test_if_interval_negative_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["interval"] = -5

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_interval_and_start_defined_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["start"] = 1 * 10 ** 9

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_interval_and_stop_defined_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["stop"] = 1 * 10 ** 9

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_does_not_contains_histogram_then_none_found(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"]

        _, _, hists = parse_config(config)

        assert len(hists) == 0
