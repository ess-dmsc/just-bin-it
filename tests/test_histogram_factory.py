import pytest
from just_bin_it.histograms.histogram_factory import HistogramFactory
from just_bin_it.histograms.histogram1d import Histogram1d
from just_bin_it.histograms.histogram2d import Histogram2d
from just_bin_it.histograms.single_event_histogram1d import SingleEventHistogram1d
from just_bin_it.histograms.det_histogram import DetHistogram


VALID_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [20, 2000],
            "num_bins": 50,
            "topic": "topic0",
            "source": "source1",
        },
        {
            "type": "hist2d",
            "tof_range": [30, 3000],
            "det_range": [40, 4000],
            "num_bins": 100,
            "topic": "topic1",
        },
        {
            "type": "sehist1d",
            "tof_range": [0, 3000],
            "num_bins": 200,
            "topic": "topic2",
            "source": "source2",
        },
        {
            "type": "dethist",
            "tof_range": [0, 4000],
            "det_range": [0, 99],
            "width": 10,
            "height": 10,
            "topic": "topic3",
            "source": "source3",
        },
    ],
}

VALID_CONFIG_WITH_ID = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [20, 2000],
            "num_bins": 50,
            "topic": "topic0",
            "source": "source1",
            "id": "123456",
        },
        {
            "type": "hist2d",
            "tof_range": [30, 3000],
            "det_range": [40, 4000],
            "num_bins": 100,
            "topic": "topic1",
        },
    ],
}


MISSING_TOF_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "histograms": [
        {"type": "hist1d", "num_bins": 50, "topic": "topic0", "source": "source1"}
    ],
}


MISSING_BINS_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 3000],
            "topic": "topic0",
            "source": "source1",
        }
    ],
}


MISSING_TOF_AND_BINS_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "histograms": [{"type": "hist1d", "topic": "topic0", "source": "source1"}],
}

MISSING_HISTOGRAMS_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
}


class TestHistogramFactory:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_creates_histograms_correctly(self):
        histograms = HistogramFactory.generate(VALID_CONFIG)

        assert isinstance(histograms[0], Histogram1d)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].num_bins == 50
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

        assert isinstance(histograms[1], Histogram2d)
        assert histograms[1].tof_range == (30, 3000)
        assert histograms[1].det_range == (40, 4000)
        assert histograms[1].num_bins == 100
        assert histograms[1].topic == "topic1"

        assert isinstance(histograms[2], SingleEventHistogram1d)
        assert histograms[2].tof_range == (0, 3000)
        assert histograms[2].num_bins == 200
        assert histograms[2].topic == "topic2"
        assert histograms[2].source == "source2"

        assert isinstance(histograms[3], DetHistogram)
        assert histograms[3].tof_range == (0, 4000)
        assert histograms[3].num_bins == 100
        assert histograms[3].height == 10
        assert histograms[3].width == 10
        assert histograms[3].topic == "topic3"
        assert histograms[3].source == "source3"

    def test_if_tof_missing_then_histogram_not_created(self):
        histograms = HistogramFactory.generate(MISSING_TOF_CONFIG)

        assert len(histograms) == 0

    def test_if_bins_missing_then_histogram_not_created(self):
        histograms = HistogramFactory.generate(MISSING_BINS_CONFIG)

        assert len(histograms) == 0

    def test_if_bins_and_tof_missing_then_histogram_not_created(self):
        histograms = HistogramFactory.generate(MISSING_TOF_AND_BINS_CONFIG)

        assert len(histograms) == 0

    def test_when_no_histograms_defined_nothing_happens(self):
        histograms = HistogramFactory.generate(MISSING_HISTOGRAMS_CONFIG)

        assert len(histograms) == 0

    def test_if_no_id_specified_then_empty_string(self):
        histograms = HistogramFactory.generate(VALID_CONFIG)

        assert histograms[0].identifier == ""

    def test_config_with_id_specified_sets_id(self):
        histograms = HistogramFactory.generate(VALID_CONFIG_WITH_ID)

        assert histograms[0].identifier == "123456"

    # TODO: More tests for when data is missing/invalid for 2d plots
