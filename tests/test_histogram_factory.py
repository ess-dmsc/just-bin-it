from copy import deepcopy
import pytest

from just_bin_it.histograms.det_histogram import DetHistogram
from just_bin_it.histograms.histogram1d import Histogram1d
from just_bin_it.histograms.histogram2d import Histogram2d
from just_bin_it.histograms.histogram_factory import HistogramFactory


CONFIG_1D = [
    {
        "data_brokers": ["localhost:9092", "someserver:9092"],
        "data_topics": ["my_topic"],
        "type": "hist1d",
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
        "type": "hist2d",
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
        "type": "dethist",
        "tof_range": [20, 2000],
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


class TestHistogramFactory1D:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = deepcopy(CONFIG_1D)

    def test_creates_histogram_correctly(self):
        histograms = HistogramFactory.generate(self.config)

        assert isinstance(histograms[0], Histogram1d)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].det_range == (0, 500)
        assert histograms[0].num_bins == 50
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

    def test_if_tof_missing_then_histogram_not_created(self):
        config = deepcopy(self.config)
        del config[0]["tof_range"]

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_tof_is_not_two_values_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["tof_range"] = (1,)

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_bins_missing_then_histogram_not_created(self):
        config = deepcopy(self.config)
        del config[0]["num_bins"]

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_bins_not_numeric_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["num_bins"] = "hello"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_det_range_is_not_two_values_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["det_range"] = (1,)

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_no_id_specified_then_empty_string(self):
        config = deepcopy(self.config)
        del config[0]["id"]

        histograms = HistogramFactory.generate(config)

        assert histograms[0].identifier == ""

    def test_config_with_id_specified_sets_id(self):
        histograms = HistogramFactory.generate(self.config)

        assert histograms[0].identifier == "123456"


class TestHistogramFactory2D:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = deepcopy(CONFIG_2D)

    def test_creates_histogram_correctly(self):
        histograms = HistogramFactory.generate(self.config)

        assert isinstance(histograms[0], Histogram2d)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].det_range == (0, 500)
        assert histograms[0].num_bins == 50
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

    def test_if_tof_missing_then_histogram_not_created(self):
        config = deepcopy(self.config)
        del config[0]["tof_range"]

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_tof_is_not_two_values_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["tof_range"] = (1,)

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_bins_missing_then_histogram_not_created(self):
        config = deepcopy(self.config)
        del config[0]["num_bins"]

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_bins_not_numeric_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["num_bins"] = "hello"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_det_range_is_not_two_values_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["det_range"] = (1,)

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_no_id_specified_then_empty_string(self):
        config = deepcopy(self.config)
        del config[0]["id"]

        histograms = HistogramFactory.generate(config)

        assert histograms[0].identifier == ""

    def test_config_with_id_specified_sets_id(self):
        histograms = HistogramFactory.generate(self.config)

        assert histograms[0].identifier == "123456"


class TestHistogramFactory2DMap:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.config = deepcopy(CONFIG_2D_MAP)

    def test_creates_histogram_correctly(self):
        histograms = HistogramFactory.generate(self.config)

        assert isinstance(histograms[0], DetHistogram)
        assert histograms[0].tof_range == (20, 2000)
        assert histograms[0].det_range == (1, 6144)
        assert histograms[0].width == 32
        assert histograms[0].height == 192
        assert histograms[0].topic == "topic0"
        assert histograms[0].source == "source1"

    def test_if_tof_missing_then_histogram_not_created(self):
        config = deepcopy(self.config)
        del config[0]["tof_range"]

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_tof_is_not_two_values_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["tof_range"] = (1,)

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_width_not_numeric_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["width"] = "hello"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_height_not_numeric_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["height"] = "hello"

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_det_range_is_not_two_values_then_histogram_not_created(self):
        config = deepcopy(self.config)
        config[0]["det_range"] = (1,)

        histograms = HistogramFactory.generate(config)

        assert len(histograms) == 0

    def test_if_no_id_specified_then_empty_string(self):
        config = deepcopy(self.config)
        del config[0]["id"]

        histograms = HistogramFactory.generate(config)

        assert histograms[0].identifier == ""

    def test_config_with_id_specified_sets_id(self):
        histograms = HistogramFactory.generate(self.config)

        assert histograms[0].identifier == "123456"
