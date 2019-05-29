import pytest
from histograms.histogrammer import Histogrammer, HISTOGRAM_STATES
from endpoints.serialisation import deserialise_hs00
from tests.mock_producer import MockProducer


START_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
        }
    ],
}

NO_HIST_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "start": 1000,
}

STOP_CONFIG = {
    "cmd": "config",
    "data_brokers": ["fakehost:9092"],
    "data_topics": ["LOQ_events"],
    "stop": 1001,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "hist-topic2",
        }
    ],
}

# Data in each "pulse" increases by factor of 2, that way we can know which
# messages were consumed by looking at the histogram sum.
EVENT_DATA = [
    {"pulse_time": 998, "tofs": [1], "det_ids": None, "source": "simulator"},
    {"pulse_time": 999, "tofs": [1, 2], "det_ids": None, "source": "simulator"},
    {"pulse_time": 1000, "tofs": [1, 2, 3, 4], "det_ids": None, "source": "simulator"},
    {
        "pulse_time": 1001,
        "tofs": [1, 2, 3, 4, 5, 6, 7, 8],
        "det_ids": None,
        "source": "simulator",
    },
    {
        "pulse_time": 1002,
        "tofs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        "det_ids": None,
        "source": "simulator",
    },
]


class TestMain:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.mock_producer = MockProducer()
        self.histogrammer = Histogrammer(self.mock_producer, START_CONFIG)

    def test_if_config_contains_histograms_then_they_are_created(self):
        assert len(self.histogrammer.histograms) == 1
        assert self.histogrammer.hist_sink is not None

    def test_if_config_does_not_contains_histogram_then_none_created(self):
        self.histogrammer = Histogrammer(self.mock_producer, NO_HIST_CONFIG)

        assert len(self.histogrammer.histograms) == 0

    def test_data_only_added_if_pulse_time_is_later_or_equal_to_start(self):
        self.histogrammer.add_data(EVENT_DATA)

        assert sum(self.histogrammer.histograms[0].data) == 28

    def test_data_only_add_up_to_stop_time(self):
        self.histogrammer = Histogrammer(self.mock_producer, STOP_CONFIG)
        self.histogrammer.add_data(EVENT_DATA)

        assert sum(self.histogrammer.histograms[0].data) == 15

    def test_while_counting_published_histogram_is_labeled_to_indicate_counting(self):
        self.histogrammer = Histogrammer(self.mock_producer, START_CONFIG)
        self.histogrammer.add_data(EVENT_DATA)

        self.histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        assert data["info"] == HISTOGRAM_STATES["COUNTING"]

    def test_after_stop_published_histogram_is_labeled_to_indicate_finished(self):
        self.histogrammer = Histogrammer(self.mock_producer, STOP_CONFIG)
        self.histogrammer.add_data(EVENT_DATA)

        self.histogrammer.publish_histograms()

        data = deserialise_hs00(self.mock_producer.messages[0][1])
        assert data["info"] == HISTOGRAM_STATES["FINISHED"]

    def test_after_stop_publishing_final_histograms_published_once_only(self):
        self.histogrammer = Histogrammer(self.mock_producer, STOP_CONFIG)
        self.histogrammer.add_data(EVENT_DATA)

        self.histogrammer.publish_histograms()
        # After stop these additional requests to publish should be ignored.
        self.histogrammer.publish_histograms()
        self.histogrammer.publish_histograms()

        assert len(self.mock_producer.messages) == 1

    def test_get_stats_returns_correct_stats(self):
        self.histogrammer.add_data(EVENT_DATA)

        stats = self.histogrammer.get_histogram_stats()

        assert stats[0]["last_pulse_time"] == 1002
        assert stats[0]["sum"] == 28

    def test_get_stats_with_no_histogram_returns_empty(self):
        self.histogrammer = Histogrammer(self.mock_producer, NO_HIST_CONFIG)

        stats = self.histogrammer.get_histogram_stats()

        assert len(stats) == 0
