import copy
import json
import pytest
from just_bin_it.endpoints.serialisation import deserialise_hs00
from just_bin_it.histograms.histogrammer import HISTOGRAM_STATES, create_histogrammer
from tests.mock_producer import MockProducer


START_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000 * 10 ** 3,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
    ],
}

START_2D_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000 * 10 ** 3,
    "histograms": [
        {
            "type": "hist2d",
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic1",
            "id": "abcdef",
        },
        {
            "type": "hist2d",
            "tof_range": [0, 100000000],
            "det_range": [0, 100],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "ghijk",
        },
    ],
}

NO_HIST_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000 * 10 ** 3,
}

STOP_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "stop": 1001 * 10 ** 3,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
        }
    ],
}

INTERVAL_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "interval": 5,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
            "id": "abcdef",
        }
    ],
}

# Data in each "pulse" increases by factor of 2, that way we can know which
# messages were consumed by looking at the histogram sum.
EVENT_DATA = [
    (
        998 * 10 ** 3,
        0,
        {
            "pulse_time": 998 * 10 ** 9,
            "tofs": [1],
            "det_ids": None,
            "source": "simulator",
        },
    ),
    (
        999 * 10 ** 3,
        1,
        {
            "pulse_time": 999 * 10 ** 9,
            "tofs": [1, 2],
            "det_ids": [1, 2],
            "source": "simulator",
        },
    ),
    (
        1000 * 10 ** 3,
        2,
        {
            "pulse_time": 1000 * 10 ** 9,
            "tofs": [1, 2, 3, 4],
            "det_ids": [1, 2, 3, 4],
            "source": "simulator",
        },
    ),
    (
        1001 * 10 ** 3,
        3,
        {
            "pulse_time": 1001 * 10 ** 9,
            "tofs": [1, 2, 3, 4, 5, 6, 7, 8],
            "det_ids": [1, 2, 3, 4, 5, 6, 7, 8],
            "source": "simulator",
        },
    ),
    (
        1002 * 10 ** 3,
        4,
        {
            "pulse_time": 1002 * 10 ** 9,
            "tofs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "det_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "source": "simulator",
        },
    ),
]

UNORDERED_EVENT_DATA = [
    (
        1000 * 10 ** 3,
        0,
        {
            "pulse_time": 1000 * 10 ** 9,
            "tofs": [1, 2, 3, 4],
            "det_ids": None,
            "source": "simulator",
        },
    ),
    (
        1001 * 10 ** 3,
        1,
        {
            "pulse_time": 1001 * 10 ** 9,
            "tofs": [1, 2, 3, 4, 5, 6, 7, 8],
            "det_ids": None,
            "source": "simulator",
        },
    ),
    (
        1002 * 10 ** 3,
        2,
        {
            "pulse_time": 1002 * 10 ** 9,
            "tofs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "det_ids": None,
            "source": "simulator",
        },
    ),
    (
        998 * 10 ** 3,
        3,
        {
            "pulse_time": 998 * 10 ** 9,
            "tofs": [1],
            "det_ids": None,
            "source": "simulator",
        },
    ),
    (
        999 * 10 ** 3,
        4,
        {
            "pulse_time": 999 * 10 ** 9,
            "tofs": [1, 2],
            "det_ids": None,
            "source": "simulator",
        },
    ),
]


class TestHistogrammer:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.mock_producer = MockProducer()

    def test_if_config_contains_histograms_then_they_are_created(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        assert len(histogrammer.histograms) == 2
        assert histogrammer.hist_sink is not None

    def test_if_config_does_not_contains_histogram_then_none_created(self):
        histogrammer = create_histogrammer(self.mock_producer, NO_HIST_CONFIG)

        assert len(histogrammer.histograms) == 0

    def test_data_only_added_if_pulse_time_is_later_or_equal_to_start(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 28
        assert histogrammer.histograms[1].data.sum() == 28

    def test_histograms_are_zero_if_all_data_before_start(self):
        config = copy.deepcopy(START_CONFIG)
        config["start"] = 1100 * 10 ** 3
        histogrammer = create_histogrammer(self.mock_producer, config)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 0

    def test_data_only_add_up_to_stop_time(self):
        histogrammer = create_histogrammer(self.mock_producer, STOP_CONFIG)

        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 15

    def test_histograms_are_zero_if_all_data_later_than_stop(self):
        config = copy.deepcopy(STOP_CONFIG)
        config["stop"] = 900 * 10 ** 3
        histogrammer = create_histogrammer(self.mock_producer, config)
        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 0

    def test_data_out_of_order_does_not_add_data_before_start(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)

        histogrammer.add_data(UNORDERED_EVENT_DATA)

        assert histogrammer.histograms[0].data.sum() == 28
        assert histogrammer.histograms[1].data.sum() == 28

    def test_before_counting_published_histogram_is_labelled_to_indicate_not_started(
        self
    ):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)

        histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        info = json.loads(data["info"])
        assert info["state"] == HISTOGRAM_STATES["INITIALISED"]

    def test_while_counting_published_histogram_is_labelled_to_indicate_counting(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        info = json.loads(data["info"])
        assert info["state"] == HISTOGRAM_STATES["COUNTING"]

    def test_after_stop_published_histogram_is_labelled_to_indicate_finished(self):
        histogrammer = create_histogrammer(self.mock_producer, STOP_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        info = json.loads(data["info"])
        assert info["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_after_stop_publishing_final_histograms_published_once_only(self):
        histogrammer = create_histogrammer(self.mock_producer, STOP_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.publish_histograms()
        # After stop these additional requests to publish should be ignored.
        histogrammer.publish_histograms()
        histogrammer.publish_histograms()

        assert len(self.mock_producer.messages) == 1

    def test_published_histogram_has_non_default_timestamp_set(self):
        histogrammer = create_histogrammer(self.mock_producer, STOP_CONFIG)
        histogrammer.add_data(EVENT_DATA)
        timestamp = 1234567890

        histogrammer.publish_histograms(timestamp)

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        assert data["timestamp"] == timestamp

    def test_get_stats_returns_correct_stats_1d(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        stats = histogrammer.get_histogram_stats()

        assert stats[0]["last_pulse_time"] == 1002 * 10 ** 9
        assert stats[0]["sum"] == 28
        assert stats[1]["last_pulse_time"] == 1002 * 10 ** 9
        assert stats[1]["sum"] == 28

    def test_get_stats_returns_correct_stats_2d(self):
        histogrammer = create_histogrammer(self.mock_producer, START_2D_CONFIG)
        histogrammer.add_data(EVENT_DATA, EVENT_DATA)

        stats = histogrammer.get_histogram_stats()

        assert stats[0]["last_pulse_time"] == 1002 * 10 ** 9
        assert stats[0]["sum"] == 28
        assert stats[1]["last_pulse_time"] == 1002 * 10 ** 9
        assert stats[1]["sum"] == 28

    def test_get_stats_with_no_histogram_returns_empty(self):
        histogrammer = create_histogrammer(self.mock_producer, NO_HIST_CONFIG)

        stats = histogrammer.get_histogram_stats()

        assert len(stats) == 0

    def test_if_histogram_has_id_then_that_is_added_to_the_info_field(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        info = json.loads(data["info"])
        assert info["id"] == "abcdef"

    def test_if_interval_and_start_defined_then_creation_throws(self):
        config = copy.deepcopy(INTERVAL_CONFIG)
        config["start"] = 1 * 10 ** 9

        with pytest.raises(Exception):
            create_histogrammer(self.mock_producer, config)

    def test_if_interval_and_stop_defined_then_creation_throws(self):
        config = copy.deepcopy(INTERVAL_CONFIG)
        config["stop"] = 1 * 10 ** 9

        with pytest.raises(Exception):
            create_histogrammer(self.mock_producer, config)

    def test_if_interval_defined_then_start_and_stop_are_initialised(self):
        current_time = 1000
        interval = INTERVAL_CONFIG["interval"] * 10 ** 3
        histogrammer = create_histogrammer(
            self.mock_producer, INTERVAL_CONFIG, current_time
        )

        assert histogrammer.start == current_time
        assert histogrammer.stop == current_time + interval

    def test_if_interval_negative_then_throws(self):
        config = copy.deepcopy(INTERVAL_CONFIG)
        config["interval"] = -5

        with pytest.raises(Exception):
            create_histogrammer(self.mock_producer, config)

    def test_clear_histograms_empties_all_histograms(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.clear_histograms()

        assert histogrammer.histograms[0].data.sum() == 0
        assert histogrammer.histograms[1].data.sum() == 0

    def test_if_no_data_after_start_time_and_stop_time_exceeded_histogram_is_finished(
        self
    ):
        config = copy.deepcopy(START_CONFIG)
        config["start"] = 1003 * 10 ** 3
        config["stop"] = 1005 * 10 ** 3

        histogrammer = create_histogrammer(self.mock_producer, config)
        # Supply a time significantly after the original stop time because of
        # leeway
        finished = histogrammer.check_stop_time_exceeded(config["stop"] * 1.1)

        info = histogrammer._generate_info(histogrammer.histograms[0])
        assert finished
        assert info["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_if_no_data_after_start_time_and_stop_time_not_exceeded_histogram_is_not_finished(
        self
    ):
        config = copy.deepcopy(START_CONFIG)
        config["start"] = 1003 * 10 ** 3
        config["stop"] = 1005 * 10 ** 3

        histogrammer = create_histogrammer(self.mock_producer, config)
        finished = histogrammer.check_stop_time_exceeded(config["stop"] * 0.9)

        info = histogrammer._generate_info(histogrammer.histograms[0])
        assert not finished
        assert info["state"] != HISTOGRAM_STATES["FINISHED"]
