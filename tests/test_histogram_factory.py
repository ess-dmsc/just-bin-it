import pytest
from histograms.histogram_factory import HistogramFactory
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d


class TestHistogramFactory:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = {
            "data_brokers": ["localhost:9092", "someserver:9092"],
            "data_topics": ["my_topic"],
            "histograms": [
                {
                    "num_dims": 1,
                    "det_range": [20, 2000],
                    "num_bins": 50,
                    "topic": "topic0",
                },
                {
                    "num_dims": 2,
                    "tof_range": [30, 3000],
                    "det_range": [40, 4000],
                    "num_bins": 100,
                    "topic": "topic1",
                },
            ],
        }
        import json

        print(json.dumps(self.config))

    def test_factory_creates_histograms_correctly(self):
        histograms = HistogramFactory.generate(self.config)

        assert isinstance(histograms[0], Histogrammer1d)
        assert (20, 2000) == histograms[0].det_range
        assert 50 == histograms[0].num_bins
        assert "topic0" == histograms[0].topic

        assert isinstance(histograms[1], Histogrammer2d)
        assert (30, 3000) == histograms[1].tof_range
        assert (40, 4000) == histograms[1].det_range
        assert 100 == histograms[1].num_bins
        assert "topic1" == histograms[1].topic
