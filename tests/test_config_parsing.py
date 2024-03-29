import copy

import pytest

from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE
from just_bin_it.histograms.histogram2d_map import MAP_TYPE
from just_bin_it.histograms.histogram_factory import parse_config

CONFIG_FULL = {
    "cmd": "config",
    "start": 1571831082207,
    "stop": 1571831125940,
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_1"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist_topic1",
            "id": "some_id1",
            "source": "source1",
        },
        {
            "type": TOF_2D_TYPE,
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist_topic2",
            "id": "some_id2",
            "source": "source1",
        },
        {
            "type": MAP_TYPE,
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_3"],
            "det_range": [1, 10000],
            "width": 100,
            "height": 100,
            "topic": "hist_topic3",
            "id": "some_id3",
            "source": "source3",
        },
    ],
}

CONFIG_INTERVAL = {
    "cmd": "config",
    "interval": 5,
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "data_brokers": ["localhost:9092"],
            "data_topics": ["junk_data_2"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": TOF_1D_TYPE,
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
            "type": TOF_1D_TYPE,
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
    def test_if_histograms_then_all_settings_added_correctly(self):
        start, stop, hists, _, _ = parse_config(CONFIG_FULL)

        assert len(hists) == 3
        assert start == CONFIG_FULL["start"]
        assert stop == CONFIG_FULL["stop"]

        for i, h in enumerate(hists):
            # common to all types
            assert h["data_brokers"] == CONFIG_FULL["histograms"][i]["data_brokers"]
            assert h["data_topics"] == CONFIG_FULL["histograms"][i]["data_topics"]
            assert h["type"] == CONFIG_FULL["histograms"][i]["type"]
            assert h["id"] == CONFIG_FULL["histograms"][i]["id"]
            assert h["source"] == CONFIG_FULL["histograms"][i]["source"]
            assert h["topic"] == CONFIG_FULL["histograms"][i]["topic"]

            # Different for different hist types
            if "tof_range" in CONFIG_FULL["histograms"][i]:
                assert h["tof_range"] == CONFIG_FULL["histograms"][i]["tof_range"]
            if "det_range" in CONFIG_FULL["histograms"][i]:
                assert h["det_range"] == CONFIG_FULL["histograms"][i]["det_range"]
            if "num_bins" in CONFIG_FULL["histograms"][i]:
                assert h["num_bins"] == CONFIG_FULL["histograms"][i]["num_bins"]
            if "height" in CONFIG_FULL["histograms"][i]:
                assert h["height"] == CONFIG_FULL["histograms"][i]["height"]
            if "width" in CONFIG_FULL["histograms"][i]:
                assert h["width"] == CONFIG_FULL["histograms"][i]["width"]

    def test_raises_if_config_invalid_for_1d_hist(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"][0]["data_brokers"]

        with pytest.raises(Exception):
            parse_config(config)

    def test_raises_if_config_invalid_for_2d_hist(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"][1]["data_brokers"]

        with pytest.raises(Exception):
            parse_config(config)

    def test_raises_if_config_invalid_for_2d_map(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"][2]["data_brokers"]

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_hist_type_is_unknown_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_FULL)
        config["histograms"][0]["type"] = "UNKNOWN"

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_no_start_and_stop_then_they_are_not_set(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["start"]
        del config["stop"]

        start, stop, _, _, _ = parse_config(config)

        assert start is None
        assert stop is None

    def test_if_interval_defined_then_start_and_stop_are_initialised(self):
        current_time = 123456
        start, stop, _, _, _ = parse_config(CONFIG_INTERVAL, current_time)

        assert start == current_time
        assert stop == current_time + CONFIG_INTERVAL["interval"] * 1000

    def test_if_interval_negative_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["interval"] = -5

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_interval_is_non_numeric_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["interval"] = "hello"

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_interval_and_start_defined_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["start"] = 1 * 10**9

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_interval_and_stop_defined_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["stop"] = 1 * 10**9

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_does_not_contains_histogram_then_none_found(self):
        config = copy.deepcopy(CONFIG_FULL)
        del config["histograms"]

        _, _, hists, _, _ = parse_config(config)

        assert len(hists) == 0

    def test_if_output_schema_defined_then_found(self):
        config = copy.deepcopy(CONFIG_FULL)
        config["output_schema"] = "hs01"

        _, _, _, schema, _ = parse_config(config)

        assert schema == "hs01"

    def test_if_output_schema_not_defined_then_uses_default(self):
        config = copy.deepcopy(CONFIG_FULL)

        _, _, _, schema, _ = parse_config(config)

        assert schema == "hs00"

    def test_if_output_schema_unknown_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["output_schema"] = ":: unknown ::"

        with pytest.raises(Exception):
            parse_config(config)

    def test_if_input_schema_defined_then_found(self):
        config = copy.deepcopy(CONFIG_FULL)
        config["input_schema"] = "ev44"

        _, _, _, _, schema = parse_config(config)

        assert schema == "ev44"

    def test_if_input_schema_not_defined_then_uses_default(self):
        config = copy.deepcopy(CONFIG_FULL)

        _, _, _, _, schema = parse_config(config)

        assert schema == "ev42"

    def test_if_input_schema_unknown_then_parsing_throws(self):
        config = copy.deepcopy(CONFIG_INTERVAL)
        config["input_schema"] = ":: unknown ::"

        with pytest.raises(Exception):
            parse_config(config)
