import copy

import pytest

from just_bin_it.histograms.histogram1d import TOF_1D_TYPE, validate_hist_1d
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE, validate_hist_2d
from just_bin_it.histograms.histogram2d_map import MAP_TYPE, validate_hist_2d_map
from just_bin_it.histograms.histogram2d_roi import ROI_TYPE, validate_hist_2d_roi
from just_bin_it.histograms.input_validators import (
    check_bins,
    check_data_brokers,
    check_data_topics,
    check_det_range,
    check_id,
    check_source,
    check_tof,
    check_topic,
)

CONFIG_1D = {
    "type": TOF_1D_TYPE,
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events_empty"],
    "tof_range": [0, 100_000_000],
    "det_range": [0, 100_000_000],
    "num_bins": 50,
    "topic": "hist_topic1",
    "id": "some_id1",
    "source": "some_source",
}

CONFIG_2D = {
    "type": TOF_2D_TYPE,
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events_empty"],
    "tof_range": [0, 100_000_000],
    "det_range": [0, 100],
    "num_bins": (50, 10),
    "topic": "hist_topic2",
    "id": "some_id3",
    "source": "some_source",
}

CONFIG_2D_MAP = {
    "type": MAP_TYPE,
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events_empty"],
    "det_range": [1, 10000],
    "width": 100,
    "height": 100,
    "topic": "hist_topic3",
    "id": "some_id",
    "source": "some_source",
}

CONFIG_2D_ROI = {
    "type": ROI_TYPE,
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events_empty"],
    "width": 3,
    "left_edges": [7, 12],
    "topic": "hist_topic3",
    "id": "some_id",
    "source": "some_source",
}


class TestCommonValidation:
    @pytest.mark.parametrize("tof", [(0, 100), [0, 100]])
    def test_if_tof_valid_then_passes(self, tof):
        assert check_tof(tof)

    @pytest.mark.parametrize(
        "tof", [123, ("a", "b"), (123, "b"), ("a", 123), (123, 0), (100, 200, 300)]
    )
    def test_if_tof_invalid_then_fails(self, tof):
        assert not check_tof(tof)

    @pytest.mark.parametrize("bins", [100, (100, 100), [100, 100]])
    def test_if_bins_valid_then_passes(self, bins):
        assert check_bins(bins)

    @pytest.mark.parametrize(
        "bins", ["a", (1, 2, 3), (-100, 100), (100, -100), (0, 100), (100, 0)]
    )
    def test_if_bins_invalid_then_fails(self, bins):
        assert not check_bins(bins)

    @pytest.mark.parametrize(
        "topic",
        [
            "simple",
            "CAPITALS",
            "under_scores",
            "dot.dot",
            "numbers123",
            "hyphen-hyphen",
        ],
    )
    def test_if_topic_valid_then_passes(self, topic):
        assert check_topic(topic)

    @pytest.mark.parametrize("topic", [123, "with spaces", "::"])
    def test_if_topic_not_valid_then_fails(self, topic):
        assert not check_topic(topic)

    def test_if_id_is_valid_then_passes(self):
        assert check_id(":: a string ::")

    @pytest.mark.parametrize("hist_id", [123, ["list"]])
    def test_if_id_is_invalid_then_fails(self, hist_id):
        assert not check_id(hist_id)

    @pytest.mark.parametrize(
        "det_range", [123, ("a", "b"), (123, "b"), ("a", 123), (123, 0)]
    )
    def test_if_det_range_invalid_then_validation_fails(self, det_range):
        assert not check_det_range(det_range)

    def test_if_source_is_valid_then_passes(self):
        assert check_source(":: a string ::")

    @pytest.mark.parametrize("hist_id", [123, ["list"]])
    def test_if_source_is_invalid_then_fails(self, hist_id):
        assert not check_source(hist_id)

    def test_if_data_topics_valid_then_passes(self):
        assert check_data_topics(["valid1", "valid2"])

    @pytest.mark.parametrize("topics", [123, [123]])
    def test_if_data_topics_invalid_then_fails(self, topics):
        assert not check_data_topics(topics)

    def test_if_data_brokers_valid_then_passes(self):
        assert check_data_brokers(["valid1", "valid2"])

    @pytest.mark.parametrize("brokers", [123, [123]])
    def test_if_data_brokers_invalid_then_fails(self, brokers):
        assert not check_data_brokers(brokers)


class TestConfigValidation1dHist:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = copy.deepcopy(CONFIG_1D)

    def test_valid_config(self):
        assert validate_hist_1d(self.config)

    @pytest.mark.parametrize(
        "missing",
        ["tof_range", "num_bins", "topic", "data_topics", "data_brokers", "type"],
    )
    def test_if_required_parameter_missing_then_validation_fails(self, missing):
        del self.config[missing]

        assert not validate_hist_1d(self.config)

    def test_if_hist_type_invalid_fails(self):
        self.config["type"] = "not correct"

        assert not validate_hist_1d(self.config)

    def test_if_tof_invalid_then_validation_fails(self):
        self.config["tof_range"] = "string"

        assert not validate_hist_1d(self.config)

    def test_if_num_bins_invalid_then_validation_fails(self):
        self.config["num_bins"] = "string"

        assert not validate_hist_1d(self.config)

    def test_if_topic_invalid_then_validation_fails(self):
        self.config["topic"] = 123

        assert not validate_hist_1d(self.config)

    @pytest.mark.parametrize("missing", ["det_range", "id", "source"])
    def test_if_optional_parameter_missing_then_validation_passes(self, missing):
        del self.config[missing]

        assert validate_hist_1d(self.config)

    def test_if_det_range_invalid_then_validation_fails(self):
        self.config["det_range"] = 123

        assert not validate_hist_1d(self.config)

    def test_if_id_invalid_then_validation_fails(self):
        self.config["id"] = 123

        assert not validate_hist_1d(self.config)

    def test_if_data_topic_invalid_then_validation_fails(self):
        self.config["data_topics"] = [123]

        assert not validate_hist_1d(self.config)

    def test_if_data_broker_invalid_then_validation_fails(self):
        self.config["data_brokers"] = [123]

        assert not validate_hist_1d(self.config)


class TestConfigValidation2dHist:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = copy.deepcopy(CONFIG_2D)

    def test_valid_config(self):
        assert validate_hist_2d(self.config)

    @pytest.mark.parametrize(
        "missing",
        [
            "tof_range",
            "num_bins",
            "topic",
            "data_topics",
            "data_brokers",
            "det_range",
            "type",
        ],
    )
    def test_if_required_parameter_missing_then_validation_fails(self, missing):
        del self.config[missing]

        assert not validate_hist_2d(self.config)

    def test_if_hist_type_invalid_fails(self):
        self.config["type"] = "not correct"

        assert not validate_hist_2d(self.config)

    def test_if_tof_invalid_then_validation_fails(self):
        self.config["tof_range"] = "string"

        assert not validate_hist_2d(self.config)

    def test_if_num_bins_invalid_then_validation_fails(self):
        self.config["num_bins"] = "string"

        assert not validate_hist_2d(self.config)

    def test_if_topic_invalid_then_validation_fails(self):
        self.config["topic"] = 123

        assert not validate_hist_2d(self.config)

    def test_if_det_range_invalid_then_validation_fails(self):
        self.config["det_range"] = 123

        assert not validate_hist_2d(self.config)

    @pytest.mark.parametrize("missing", ["id", "source"])
    def test_if_optional_parameter_missing_then_validation_passes(self, missing):
        del self.config[missing]

        assert validate_hist_2d(self.config)

    def test_if_id_invalid_then_validation_fails(self):
        self.config["id"] = 123

        assert not validate_hist_2d(self.config)

    def test_if_data_topic_invalid_then_validation_fails(self):
        self.config["data_topics"] = [123]

        assert not validate_hist_2d(self.config)

    def test_if_data_broker_invalid_then_validation_fails(self):
        self.config["data_brokers"] = [123]

        assert not validate_hist_2d(self.config)


class TestConfigValidation2dMap:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = copy.deepcopy(CONFIG_2D_MAP)

    def test_valid_config(self):
        assert validate_hist_2d_map(self.config)

    @pytest.mark.parametrize(
        "missing",
        [
            "topic",
            "data_topics",
            "data_brokers",
            "det_range",
            "width",
            "height",
            "type",
        ],
    )
    def test_if_required_parameter_missing_then_validation_fails(self, missing):
        del self.config[missing]

        assert not validate_hist_2d_map(self.config)

    def test_if_hist_type_invalid_fails(self):
        self.config["type"] = "not correct"

        assert not validate_hist_2d_map(self.config)

    @pytest.mark.parametrize("width", ["string", 0, -100, [100]])
    def test_if_width_invalid_then_validation_fails(self, width):
        self.config["width"] = width

        assert not validate_hist_2d_map(self.config)

    @pytest.mark.parametrize("height", ["string", 0, -100, [100]])
    def test_if_height_invalid_then_validation_fails(self, height):
        self.config["width"] = height

        assert not validate_hist_2d_map(self.config)

    def test_if_topic_invalid_then_validation_fails(self):
        self.config["topic"] = 123

        assert not validate_hist_2d_map(self.config)

    def test_if_det_range_invalid_then_validation_fails(self):
        self.config["det_range"] = 123

        assert not validate_hist_2d_map(self.config)

    @pytest.mark.parametrize("missing", ["id", "source"])
    def test_if_optional_parameter_missing_then_validation_passes(self, missing):
        del self.config[missing]

        assert validate_hist_2d_map(self.config)

    def test_if_id_invalid_then_validation_fails(self):
        self.config["id"] = 123

        assert not validate_hist_2d_map(self.config)

    def test_if_data_topic_invalid_then_validation_fails(self):
        self.config["data_topics"] = [123]

        assert not validate_hist_2d_map(self.config)

    def test_if_data_broker_invalid_then_validation_fails(self):
        self.config["data_brokers"] = [123]

        assert not validate_hist_2d_map(self.config)


class TestConfigValidation2dRoi:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = copy.deepcopy(CONFIG_2D_ROI)

    def test_valid_config(self):
        assert validate_hist_2d_roi(self.config)

    @pytest.mark.parametrize(
        "missing",
        ["topic", "data_topics", "data_brokers", "width", "type", "left_edges"],
    )
    def test_if_required_parameter_missing_then_validation_fails(self, missing):
        del self.config[missing]

        assert not validate_hist_2d_roi(self.config)

    def test_if_hist_type_invalid_fails(self):
        self.config["type"] = "not correct"

        assert not validate_hist_2d_roi(self.config)

    @pytest.mark.parametrize("width", ["string", 0, -100, [100]])
    def test_if_width_invalid_then_validation_fails(self, width):
        self.config["width"] = width

        assert not validate_hist_2d_roi(self.config)

    @pytest.mark.parametrize("height", ["string", 0, -100, [100]])
    def test_if_height_invalid_then_validation_fails(self, height):
        self.config["width"] = height

        assert not validate_hist_2d_roi(self.config)

    def test_if_topic_invalid_then_validation_fails(self):
        self.config["topic"] = 123

        assert not validate_hist_2d_roi(self.config)

    @pytest.mark.parametrize(
        "left_edges",
        (123, [], [1, "abc", 3]),
        ids=("not a list", "empty_list", "non-numeric value"),
    )
    def test_if_left_edges_invalid_then_validation_fails(self, left_edges):
        self.config["left_edges"] = left_edges

        assert not validate_hist_2d_roi(self.config)

    @pytest.mark.parametrize("missing", ["id", "source"])
    def test_if_optional_parameter_missing_then_validation_passes(self, missing):
        del self.config[missing]

        assert validate_hist_2d_roi(self.config)

    def test_if_id_invalid_then_validation_fails(self):
        self.config["id"] = 123

        assert not validate_hist_2d_roi(self.config)

    def test_if_data_topic_invalid_then_validation_fails(self):
        self.config["data_topics"] = [123]

        assert not validate_hist_2d_roi(self.config)

    def test_if_data_broker_invalid_then_validation_fails(self):
        self.config["data_brokers"] = [123]

        assert not validate_hist_2d_roi(self.config)
