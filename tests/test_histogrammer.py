import pytest
import json
import copy
from histograms.histogrammer import HISTOGRAM_STATES, create_histogrammer
from endpoints.serialisation import deserialise_hs00
from tests.mock_producer import MockProducer


START_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000 * 10 ** 9,
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

NO_HIST_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000 * 10 ** 9,
}

STOP_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "stop": 1001 * 10 ** 9,
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
    {"pulse_time": 998 * 10 ** 9, "tofs": [1], "det_ids": None, "source": "simulator"},
    {
        "pulse_time": 999 * 10 ** 9,
        "tofs": [1, 2],
        "det_ids": None,
        "source": "simulator",
    },
    {
        "pulse_time": 1000 * 10 ** 9,
        "tofs": [1, 2, 3, 4],
        "det_ids": None,
        "source": "simulator",
    },
    {
        "pulse_time": 1001 * 10 ** 9,
        "tofs": [1, 2, 3, 4, 5, 6, 7, 8],
        "det_ids": None,
        "source": "simulator",
    },
    {
        "pulse_time": 1002 * 10 ** 9,
        "tofs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        "det_ids": None,
        "source": "simulator",
    },
]

UNORDERED_EVENT_DATA = [
    {
        "pulse_time": 1000 * 10 ** 9,
        "tofs": [1, 2, 3, 4],
        "det_ids": None,
        "source": "simulator",
    },
    {
        "pulse_time": 1001 * 10 ** 9,
        "tofs": [1, 2, 3, 4, 5, 6, 7, 8],
        "det_ids": None,
        "source": "simulator",
    },
    {
        "pulse_time": 1002 * 10 ** 9,
        "tofs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        "det_ids": None,
        "source": "simulator",
    },
    {"pulse_time": 998 * 10 ** 9, "tofs": [1], "det_ids": None, "source": "simulator"},
    {
        "pulse_time": 999 * 10 ** 9,
        "tofs": [1, 2],
        "det_ids": None,
        "source": "simulator",
    },
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

        assert sum(histogrammer.histograms[0].data) == 28
        assert sum(histogrammer.histograms[1].data) == 28

    def test_data_only_add_up_to_stop_time(self):
        histogrammer = create_histogrammer(self.mock_producer, STOP_CONFIG)

        histogrammer.add_data(EVENT_DATA)

        assert sum(histogrammer.histograms[0].data) == 15

    def test_data_out_of_order_does_not_add_data_before_start(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)

        histogrammer.add_data(UNORDERED_EVENT_DATA)

        assert sum(histogrammer.histograms[0].data) == 28
        assert sum(histogrammer.histograms[1].data) == 28

    def test_before_counting_published_histogram_is_labelled_to_indicate_not_started(
        self
    ):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)

        histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        info = json.loads(data["info"])
        assert info["state"] == HISTOGRAM_STATES["NOT_STARTED"]

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

    def test_get_stats_returns_correct_stats(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

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

    def test_if_interval_defined_then_start_and_stop_are_not_initialised(self):
        histogrammer = create_histogrammer(self.mock_producer, INTERVAL_CONFIG)

        assert histogrammer.start is None
        assert histogrammer.stop is None

    def test_if_interval_defined_then_start_and_stop_are_initialised_after_first_data(
        self
    ):
        histogrammer = create_histogrammer(self.mock_producer, INTERVAL_CONFIG)

        histogrammer.add_data(EVENT_DATA)

        assert histogrammer.start == 998 * 10 ** 9
        assert histogrammer.stop == 1003 * 10 ** 9

    def test_if_interval_negative_then_throws(self):
        config = copy.deepcopy(INTERVAL_CONFIG)
        config["interval"] = -5

        with pytest.raises(Exception):
            create_histogrammer(self.mock_producer, config)

    def test_interval_is_converted_to_nanoseconds(self):
        histogrammer = create_histogrammer(self.mock_producer, INTERVAL_CONFIG)

        assert histogrammer.interval == 5 * 10 ** 9

    def test_clear_histograms_empties_all_histograms(self):
        histogrammer = create_histogrammer(self.mock_producer, START_CONFIG)
        histogrammer.add_data(EVENT_DATA)

        histogrammer.clear_histograms()

        assert sum(histogrammer.histograms[0].data) == 0
        assert sum(histogrammer.histograms[1].data) == 0
