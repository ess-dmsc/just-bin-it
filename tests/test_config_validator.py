import copy
import numbers
import re

import pytest

CONFIG_1D = {
    "type": "hist1d",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["fake_events_empty"],
    "tof_range": [0, 100_000_000],
    "det_range": [0, 100_000_000],
    "num_bins": 50,
    "topic": "hist_topic1",
    "id": "some_id1",
}


def check_tof(tof):
    if not isinstance(tof, (list, tuple)) or len(tof) != 2:
        return False
    if not isinstance(tof[0], numbers.Number) or not isinstance(tof[1], numbers.Number):
        return False
    if tof[0] > tof[1]:
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


def validate_hist_1d(histogram_config):
    required = ["tof_range", "num_bins", "topic"]
    if any(req not in histogram_config for req in required):
        return False

    if not check_tof(histogram_config["tof_range"]):
        return False

    if not check_bins(histogram_config["num_bins"]):
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    return True


class TestConfigValidationHist1d:
    def test_valid_config(self):
        config = copy.deepcopy(CONFIG_1D)
        assert validate_hist_1d(config)

    @pytest.mark.parametrize("missing", ["tof_range", "num_bins", "topic"])
    def test_if_required_parameter_missing_then_validation_fails(self, missing):
        config = copy.deepcopy(CONFIG_1D)
        del config[missing]

        assert not validate_hist_1d(config)


class TestCommonValidation:
    @pytest.mark.parametrize("tof", [(0, 100), [0, 100]])
    def test_if_tof_valid_then_validation_passes(self, tof):
        assert check_tof(tof)

    @pytest.mark.parametrize("tof", [123, ("a", "b"), (123, "b"), ("a", 123), (123, 0)])
    def test_if_tof_invalid_then_validation_fails(self, tof):
        assert not check_tof(tof)

    @pytest.mark.parametrize("bins", [100, (100, 100), [100, 100]])
    def test_if_bins_valid_then_validation_passes(self, bins):
        assert check_bins(bins)

    @pytest.mark.parametrize(
        "bins", ["a", (1, 2, 3), (-100, 100), (100, -100), (0, 100), (100, 0)]
    )
    def test_if_bins_invalid_then_validation_fails(self, bins):
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
    def test_if_topic_valid_string_then_validation_passes(self, topic):
        assert check_topic(topic)

    @pytest.mark.parametrize("topic", [123, "with spaces", "::"])
    def test_if_topic_not_valid_string_then_validation_fails(self, topic):
        assert not check_topic(topic)

    # def test_if_tof_is_not_two_values_then_histogram_not_created(self):
    #     with pytest.raises(JustBinItException):
    #         Histogram1d(IRRELEVANT_TOPIC, IRRELEVANT_NUM_BINS, (1,))
    #
    # def test_if_bins_not_numeric_then_histogram_not_created(self):
    #     with pytest.raises(JustBinItException):
    #         Histogram1d(IRRELEVANT_TOPIC, None, IRRELEVANT_TOF_RANGE)
    #
    # def test_if_bins_not_greater_than_zero_then_histogram_not_created(self):
    #     with pytest.raises(JustBinItException):
    #         Histogram1d(IRRELEVANT_TOPIC, 0, IRRELEVANT_TOF_RANGE)
    #
    # def test_if_det_range_is_not_two_values_then_histogram_not_created(self):
    #     with pytest.raises(JustBinItException):
    #         Histogram1d(
    #             IRRELEVANT_TOPIC, IRRELEVANT_NUM_BINS, IRRELEVANT_TOF_RANGE, (1,)
    #         )
    #
    # def test_if_no_id_specified_then_empty_string(self):
    #     histogram = Histogram1d(
    #         IRRELEVANT_TOPIC, IRRELEVANT_NUM_BINS, IRRELEVANT_TOF_RANGE
    #     )
    #
    #     assert histogram.identifier == ""
    #
    # def test_config_with_id_specified_sets_id(self):
    #     histogram = Histogram1d(
    #         IRRELEVANT_TOPIC,
    #         IRRELEVANT_NUM_BINS,
    #         IRRELEVANT_TOF_RANGE,
    #         identifier="123456",
    #     )
    #
    #     assert histogram.identifier == "123456"
