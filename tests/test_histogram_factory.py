import pytest
from histograms.histogram_factory import HistogramFactory
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d
from histograms.single_event_histogrammer1d import SingleEventHistogrammer1d


class TestHistogramFactory:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = {
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

    def test_factory_creates_histograms_correctly(self):
        histograms = HistogramFactory.generate(self.config)

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

    # TODO: More tests for when data is missing
