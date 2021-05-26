import copy
import numbers
import re

import pytest

from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE
from just_bin_it.histograms.histogram2d_map import MAP_TYPE

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


def check_tof(tof):
    if not isinstance(tof, (list, tuple)) or len(tof) != 2:
        return False
    if not isinstance(tof[0], numbers.Number) or not isinstance(tof[1], numbers.Number):
        return False
    if tof[0] > tof[1]:
        return False
    return True


def check_det_range(det_range):
    if not isinstance(det_range, (list, tuple)) or len(det_range) != 2:
        return False
    if not isinstance(det_range[0], numbers.Number) or not isinstance(
        det_range[1], numbers.Number
    ):
        return False
    if det_range[0] > det_range[1]:
        return False
    return True


def check_bins(num_bins):
    if isinstance(num_bins, int) and num_bins > 0:
        return True

    if isinstance(num_bins, (list, tuple)) and len(num_bins) == 2:
        if (
            isinstance(num_bins[0], int)
            and num_bins[0] > 0
            and isinstance(num_bins[1], int)
            and num_bins[1] > 0
        ):
            return True

    return False


def check_topic(topic):
    if not isinstance(topic, str):
        return False
    # Matching rules from Kafka documentation
    if not re.match(r"^[a-zA-Z0-9._\-]+$", topic):
        return False
    return True


def check_data_topics(topics):
    if not isinstance(topics, (list, tuple)):
        return False

    return all(check_topic(topic) for topic in topics)


def check_data_brokers(brokers):
    if not isinstance(brokers, (list, tuple)):
        return False

    # For now just check they are strings
    return all(isinstance(broker, str) for broker in brokers)


def check_id(hist_id):
    return isinstance(hist_id, str)


def check_source(source):
    return isinstance(source, str)


def validate_hist_1d(histogram_config):
    required = ["tof_range", "num_bins", "topic", "data_topics", "data_brokers", "type"]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != TOF_1D_TYPE:
        return False

    if not check_tof(histogram_config["tof_range"]):
        return False

    if not check_bins(histogram_config["num_bins"]):
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    if not check_data_topics(histogram_config["data_topics"]):
        return False

    if not check_data_brokers(histogram_config["data_brokers"]):
        return False

    if "det_range" in histogram_config and not check_det_range(
        histogram_config["det_range"]
    ):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


def validate_hist_2d(histogram_config):
    required = [
        "tof_range",
        "num_bins",
        "topic",
        "data_topics",
        "data_brokers",
        "det_range",
        "type",
    ]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != TOF_2D_TYPE:
        return False

    if not check_tof(histogram_config["tof_range"]):
        return False

    if not check_bins(histogram_config["num_bins"]):
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    if not check_data_topics(histogram_config["data_topics"]):
        return False

    if not check_data_brokers(histogram_config["data_brokers"]):
        return False

    if not check_det_range(histogram_config["det_range"]):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


def validate_hist_2d_map(histogram_config):
    required = [
        "topic",
        "data_topics",
        "data_brokers",
        "det_range",
        "width",
        "height",
        "type",
    ]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != MAP_TYPE:
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    if not check_data_topics(histogram_config["data_topics"]):
        return False

    if not check_data_brokers(histogram_config["data_brokers"]):
        return False

    if not check_det_range(histogram_config["det_range"]):
        return False

    if (
        not isinstance(histogram_config["height"], numbers.Number)
        or histogram_config["height"] < 1
    ):
        return False

    if (
        not isinstance(histogram_config["width"], numbers.Number)
        or histogram_config["width"] < 1
    ):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


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
        assert check_id(":: a string ::")

    @pytest.mark.parametrize("hist_id", [123, ["list"]])
    def test_if_source_is_invalid_then_fails(self, hist_id):
        assert not check_id(hist_id)

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


class TestConfigValidationHist1d:
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


class TestConfigValidationHist2d:
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


class TestConfigValidationMap2d:
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