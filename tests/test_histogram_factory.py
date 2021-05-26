from copy import deepcopy

from just_bin_it.histograms.histogram1d import TOF_1D_TYPE, Histogram1d
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE, Histogram2d
from just_bin_it.histograms.histogram2d_map import MAP_TYPE, DetHistogram
from just_bin_it.histograms.histogram_factory import HistogramFactory

CONFIG_1D = [
    {
        "data_brokers": ["localhost:9092", "someserver:9092"],
        "data_topics": ["my_topic"],
        "type": TOF_1D_TYPE,
        "tof_range": [20, 2000],
        "det_range": [0, 500],
        "num_bins": 50,
        "topic": "topic0",
        "source": "source1",
        "id": "123456",
    }
]

CONFIG_2D = [
    {
        "data_brokers": ["localhost:9092", "someserver:9092"],
        "data_topics": ["my_topic"],
        "type": TOF_2D_TYPE,
        "tof_range": [20, 2000],
        "det_range": [0, 500],
        "num_bins": 50,
        "topic": "topic0",
        "source": "source1",
        "id": "123456",
    }
]


CONFIG_2D_MAP = [
    {
        "data_brokers": ["localhost:9092", "someserver:9092"],
        "data_topics": ["my_topic"],
        "type": MAP_TYPE,
        "det_range": [1, 6144],
        "width": 32,
        "height": 192,
        "topic": "topic0",
        "source": "source1",
        "id": "123456",
    }
]


NO_HISTOGRAMS_CONFIG = []


class TestHistogramFactory:
    def test_when_no_histograms_defined_nothing_happens(self):
        histograms = HistogramFactory.generate(NO_HISTOGRAMS_CONFIG)

        assert len(histograms) == 0

    def test_multiple_configs_produces_multiple_histograms(self):
        config = deepcopy(CONFIG_1D)
        config.extend(deepcopy(CONFIG_2D))
        config.extend(deepcopy(CONFIG_2D_MAP))

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 3

    def test_creates_1d_histogram_correctly(self):
        histograms = HistogramFactory.generate(CONFIG_1D)

        assert isinstance(histograms[0], Histogram1d)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].det_range == (0, 500)
        assert histograms[0].num_bins == 50
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

    def test_does_not_creates_1d_histogram_on_invalid_inputs(self):
        config = deepcopy(CONFIG_1D)
        config[0]["num_bins"] = "NONSENSE"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_creates_2d_histogram_correctly(self):
        histograms = HistogramFactory.generate(CONFIG_2D)

        assert isinstance(histograms[0], Histogram2d)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].det_range == (0, 500)
        assert histograms[0].num_bins == 50
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

    def test_does_not_creates_2d_histogram_on_invalid_inputs(self):
        config = deepcopy(CONFIG_2D)
        config[0]["num_bins"] = "NONSENSE"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_creates_2d_map_histogram_correctly(self):
        histograms = HistogramFactory.generate(CONFIG_2D_MAP)

        assert isinstance(histograms[0], DetHistogram)
        assert histograms[0].det_range == (1, 6144)
        assert histograms[0].width == 32
        assert histograms[0].height == 192
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

    def test_does_not_creates_2d_map_histogram_on_invalid_inputs(self):
        config = deepcopy(CONFIG_2D_MAP)
        config[0]["det_range"] = "NONSENSE"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0
