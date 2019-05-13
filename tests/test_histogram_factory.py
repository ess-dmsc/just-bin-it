import pytest
from histograms.histogram_factory import HistogramFactory
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d
from histograms.single_event_histogrammer1d import SingleEventHistogrammer1d


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


class TestHistogramFactory:
    @pytest.fixture(autouse=True)
    def prepare(self):
        pass

    def test_creates_histograms_correctly(self):
        histograms = HistogramFactory.generate(VALID_CONFIG)

        assert isinstance(histograms[0], Histogrammer1d)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].num_bins == 50
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

        assert isinstance(histograms[1], Histogrammer2d)
        assert histograms[1].tof_range == (30, 3000)
        assert histograms[1].det_range == (40, 4000)
        assert histograms[1].num_bins == 100
        assert histograms[1].topic == "topic1"

        assert isinstance(histograms[2], SingleEventHistogrammer1d)
        assert histograms[2].tof_range == (0, 3000)
        assert histograms[2].num_bins == 200
        assert histograms[2].topic == "topic2"
        assert histograms[2].source == "source2"

    def test_throws_if_tof_missing(self):
        with pytest.raises(Exception):
            _ = HistogramFactory.generate(MISSING_TOF_CONFIG)

    def test_throws_if_bins_missing(self):
        with pytest.raises(Exception):
            _ = HistogramFactory.generate(MISSING_BINS_CONFIG)

    def test_throws_if_bins_and_tof_missing(self):
        with pytest.raises(Exception):
            _ = HistogramFactory.generate(MISSING_TOF_AND_BINS_CONFIG)

    # TODO: More tests for when data is missing
