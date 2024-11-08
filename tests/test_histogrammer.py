import copy

import numpy as np
import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME

from just_bin_it.endpoints.histogram_sink import HistogramSink
from just_bin_it.endpoints.serialisation import serialise_hs00
from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE
from just_bin_it.histograms.histogram2d_map import MAP_TYPE
from just_bin_it.histograms.histogram_factory import HistogramFactory, parse_config
from just_bin_it.histograms.histogrammer import HISTOGRAM_STATES, Histogrammer
from tests.doubles.producers import SpyProducer

START_CONFIG = {
    "cmd": "config",
    "start": 1000 * 10**3,
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "data_brokers": ["fakehost:9092"],
            "data_topics": ["LOQ_events"],
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": TOF_1D_TYPE,
            "data_brokers": ["fakehost:9092"],
            "data_topics": ["LOQ_events"],
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
    ],
}

START_2D_CONFIG = {
    "cmd": "config",
    "start": 1000 * 10**3,
    "histograms": [
        {
            "type": TOF_2D_TYPE,
            "data_brokers": ["fakehost:9092"],
            "data_topics": ["LOQ_events"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": TOF_2D_TYPE,
            "data_brokers": ["fakehost:9092"],
            "data_topics": ["LOQ_events"],
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
        {
            "type": MAP_TYPE,
            "data_brokers": ["fakehost:9092"],
            "data_topics": ["LOQ_events"],
            "det_range": [0, 100],
            "width": 100,
            "height": 100,
            "topic": "hist-topic3",
            "id": "xyzvfr",
        },
    ],
}

NO_HIST_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000 * 10**3,
}

STOP_CONFIG = {
    "cmd": "config",
    "stop": 1001 * 10**3,
    "histograms": [
        {
            "data_brokers": ["fakehost:9092"],
            "data_topics": ["LOQ_events"],
            "type": TOF_1D_TYPE,
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
        }
    ],
}


# Data in each "pulse" increases by factor of 2, that way we can know which
# messages were consumed by looking at the histogram sum.
EVENT_DATA = [
    (
        (TIMESTAMP_CREATE_TIME, 998 * 10**3),
        0,
        ("simulator", 998 * 10**9, [1], [1], None),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 999 * 10**3),
        1,
        ("simulator", 999 * 10**9, [1, 2], [1, 2], None),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 1000 * 10**3),
        2,
        ("simulator", 1000 * 10**9, [1, 2, 3, 4], [1, 2, 3, 4], None),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 1001 * 10**3),
        3,
        (
            "simulator",
            1001 * 10**9,
            [1, 2, 3, 4, 5, 6, 7, 8],
            [1, 2, 3, 4, 5, 6, 7, 8],
            None,
        ),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 1002 * 10**3),
        4,
        (
            "simulator",
            1002 * 10**9,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            None,
        ),
    ),
]

UNORDERED_EVENT_DATA = [
    (
        (TIMESTAMP_CREATE_TIME, 1000 * 10**3),
        0,
        ("simulator", 1000 * 10**9, [1, 2, 3, 4], [1, 2, 3, 4], None),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 1001 * 10**3),
        1,
        (
            "simulator",
            1001 * 10**9,
            [1, 2, 3, 4, 5, 6, 7, 8],
            [1, 2, 3, 4, 5, 6, 7, 8],
            None,
        ),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 1002 * 10**3),
        2,
        (
            "simulator",
            1002 * 10**9,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            None,
        ),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 998 * 10**3),
        3,
        ("simulator", 998 * 10**9, [1], [1], None),
    ),
    (
        (TIMESTAMP_CREATE_TIME, 999 * 10**3),
        4,
        ("simulator", 999 * 10**9, [1, 2], [1, 2], None),
    ),
]


def create_histogrammer(hist_sink, configuration):
    """
    Creates a fully configured histogrammer.

    :param hist_sink: The sink to write histograms to.
    :param configuration: The configuration message.
    :return: The created histogrammer.
    """
    start, stop, hist_configs, _, _ = parse_config(configuration)
    histograms = HistogramFactory.generate(hist_configs)

    return Histogrammer(histograms, start, stop)


def update_stats(histogrammer):
    generator = histogrammer.histogram_info()
    for _ in generator:
        pass


class TestHistogrammer:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.spy_producer = SpyProducer()
        self.hist_sink = HistogramSink(self.spy_producer, serialise_hs00)

    def test_if_config_contains_histograms_then_they_are_created(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        assert len(histogrammer.histograms) == 2

    def test_data_only_added_if_pulse_time_is_later_or_equal_to_start(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 28
        assert histogrammer.histograms[1].data.sum() == 28

    def test_histograms_are_zero_if_all_data_before_start(self):
        config = copy.deepcopy(START_CONFIG)
        config["start"] = 1100 * 10**3
        histogrammer = create_histogrammer(self.hist_sink, config)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 0

    def test_data_only_added_up_to_stop_time(self):
        histogrammer = create_histogrammer(self.hist_sink, STOP_CONFIG)

        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 15

    def test_histograms_are_zero_if_all_data_later_than_stop(self):
        config = copy.deepcopy(STOP_CONFIG)
        config["stop"] = 900 * 10**3
        histogrammer = create_histogrammer(self.hist_sink, config)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 0

    def test_data_out_of_order_does_not_add_data_before_start(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)

        histogrammer.add_data(UNORDERED_EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 28
        assert histogrammer.histograms[1].data.sum() == 28

    def test_before_counting_starts_histograms_are_labelled_as_initialised(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)

        results = list(histogrammer.histogram_info())

        assert results[0][1]["state"] == HISTOGRAM_STATES["INITIALISED"]
        assert results[1][1]["state"] == HISTOGRAM_STATES["INITIALISED"]

    def test_while_counting_histograms_are_labelled_as_counting(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        results = list(histogrammer.histogram_info())

        assert results[0][1]["state"] == HISTOGRAM_STATES["COUNTING"]
        assert results[1][1]["state"] == HISTOGRAM_STATES["COUNTING"]

    def test_after_stop_histogram_is_labelled_as_finished(self):
        histogrammer = create_histogrammer(self.hist_sink, STOP_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        results = list(histogrammer.histogram_info())

        assert results[0][1]["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_message_time_exceeds_stop_time_then_is_finished(self):
        histogrammer = create_histogrammer(self.hist_sink, STOP_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.is_finished()

    def test_message_time_before_stop_time_then_is_not_finished(self):
        config = copy.deepcopy(STOP_CONFIG)
        config["stop"] = 1003 * 10**3
        histogrammer = create_histogrammer(self.hist_sink, config)
        histogrammer.add_data(EVENT_DATA)

        assert not histogrammer.is_finished()

    def test_get_stats_returns_correct_stats_1d(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        update_stats(histogrammer)
        stats = histogrammer.get_histogram_stats()

        assert stats[0]["last_pulse_time"] == 1002 * 10**9
        assert stats[0]["sum"] == 28
        assert stats[0]["diff"] == 28
        assert np.allclose(
            stats[0]["rate"], 28 / stats[0]["last_pulse_time"] * 1e9, atol=1e-7, rtol=0
        )
        assert stats[1]["last_pulse_time"] == 1002 * 10**9
        assert stats[1]["sum"] == 28
        assert stats[1]["diff"] == 28
        assert np.allclose(
            stats[1]["rate"], 28 / stats[1]["last_pulse_time"] * 1e9, atol=1e-7, rtol=0
        )

    def test_get_stats_returns_correct_counts_since_last_request(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)
        update_stats(histogrammer)
        histogrammer.get_histogram_stats()
        histogrammer.add_data(EVENT_DATA)
        update_stats(histogrammer)
        histogrammer.get_histogram_stats()
        histogrammer.add_data(EVENT_DATA)

        update_stats(histogrammer)
        stats = histogrammer.get_histogram_stats()

        assert stats[0]["diff"] == 28
        assert stats[1]["diff"] == 28

    def test_get_stats_returns_correct_stats_2d(self):
        histogrammer = create_histogrammer(self.hist_sink, START_2D_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        update_stats(histogrammer)
        stats = histogrammer.get_histogram_stats()

        assert stats[0]["last_pulse_time"] == 1002 * 10**9
        assert stats[0]["sum"] == 28
        assert stats[0]["diff"] == 28
        assert np.allclose(
            stats[0]["rate"], 28 / stats[0]["last_pulse_time"] * 1e9, atol=1e-7, rtol=0
        )
        assert stats[1]["last_pulse_time"] == 1002 * 10**9
        assert stats[1]["sum"] == 28
        assert stats[1]["diff"] == 28
        assert np.allclose(
            stats[1]["rate"], 28 / stats[1]["last_pulse_time"] * 1e9, atol=1e-7, rtol=0
        )
        assert stats[2]["sum"] == 28
        assert stats[2]["diff"] == 28
        assert stats[2]["last_pulse_time"] == 1002 * 10**9
        assert np.allclose(
            stats[2]["rate"], 28 / stats[2]["last_pulse_time"] * 1e9, atol=1e-7, rtol=0
        )

    def test_get_stats_with_no_histogram_returns_empty(self):
        histogrammer = create_histogrammer(self.hist_sink, NO_HIST_CONFIG)

        # update_stats is not called so no stats are generated
        stats = histogrammer.get_histogram_stats()

        assert len(stats) == 0

    def test_stats_are_empty_if_no_histogram_has_been_updated(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        stats = histogrammer.get_histogram_stats()

        assert stats[0] == {}
        assert stats[1] == {}

    def test_if_histogram_has_id_then_that_is_added_to_the_info_field(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        results = list(histogrammer.histogram_info())

        assert results[0][1]["id"] == "abcdef"

    def test_clear_histograms_empties_all_histograms(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.clear_histograms()

        assert histogrammer.histograms[0].data.sum() == 0
        assert histogrammer.histograms[1].data.sum() == 0

    def test_clear_histograms_resets_statistics(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)
        update_stats(histogrammer)
        histogrammer.get_histogram_stats()

        histogrammer.clear_histograms()

        update_stats(histogrammer)
        stats = histogrammer.get_histogram_stats()

        assert stats[0]["sum"] == 0
        assert stats[0]["diff"] == 0
        assert stats[1]["sum"] == 0
        assert stats[1]["diff"] == 0
        assert stats[0]["rate"] == 0
        assert stats[1]["rate"] == 0

    def test_histogram_rate_is_zero_when_no_data(self):
        histogrammer = create_histogrammer(self.hist_sink, START_CONFIG)

        update_stats(histogrammer)
        stats = histogrammer.get_histogram_stats()

        assert stats[0]["rate"] == 0
        assert stats[1]["rate"] == 0

    def test_if_start_time_and_stop_time_defined_then_they_are_in_the_info(self):
        config = copy.deepcopy(START_CONFIG)
        config["start"] = 1003 * 10**3
        config["stop"] = 1005 * 10**3

        histogrammer = create_histogrammer(self.hist_sink, config)
        results = list(histogrammer.histogram_info())

        assert results[0][1]["start"] == 1003 * 10**3
        assert results[0][1]["stop"] == 1005 * 10**3

    def test_if_start_time_and_stop_time_not_defined_then_they_are_not_in_the_info(
        self,
    ):
        config = copy.deepcopy(START_CONFIG)
        del config["start"]

        histogrammer = create_histogrammer(self.hist_sink, config)
        results = list(histogrammer.histogram_info())

        assert "start" not in results[0][1]
        assert "stop" not in results[0][1]

    def test_if_interval_defined_then_start_and_stop_are_in_the_info(self):
        config = copy.deepcopy(START_CONFIG)
        del config["start"]
        config["interval"] = 5

        histogrammer = create_histogrammer(self.hist_sink, config)
        results = list(histogrammer.histogram_info())

        assert "start" in results[0][1]
        assert "stop" in results[0][1]
