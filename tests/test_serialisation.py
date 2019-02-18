import pytest
import numpy as np
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d
from endpoints.serialisation import serialise_hs00, deserialise_hs00


NUM_BINS = 5
X_RANGE = (0, 5)
Y_RANGE = (0, 10)
TOF_DATA = np.array([x for x in range(NUM_BINS)])
DET_DATA = np.array([x for x in range(NUM_BINS)])


def _create_1d_histogrammer():
    histogrammer = Histogrammer1d(X_RANGE, NUM_BINS, "topic")
    histogrammer.add_data(TOF_DATA)
    return histogrammer


def _create_2d_histogrammer():
    histogrammer = Histogrammer2d(X_RANGE, Y_RANGE, NUM_BINS, "topic")
    histogrammer.add_data(TOF_DATA, DET_DATA)
    return histogrammer


class TestSerialisation:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.hist_1d = _create_1d_histogrammer()
        self.hist_2d = _create_2d_histogrammer()

    def test_serialises_hs00_message_correctly_for_1d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.hist_1d)

        hist = deserialise_hs00(buf)
        assert hist["source"] == "just-bin-it"
        assert hist["shape"] == [self.hist_1d.num_bins]
        assert hist["dims"][0]["edges"] == self.hist_1d.x_edges.tolist()
        assert hist["dims"][0]["length"] == self.hist_1d.num_bins
        assert hist["dims"][0]["type"] == np.float64
        assert np.array_equal(hist["data"], self.hist_1d.histogram)

    def test_serialises_hs00_message_correctly_for_2d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.hist_2d)

        hist = deserialise_hs00(buf)
        assert hist["source"] == "just-bin-it"
        assert hist["shape"] == [self.hist_2d.num_bins, self.hist_2d.num_bins]
        assert hist["dims"][0]["edges"] == self.hist_2d.x_edges.tolist()
        assert hist["dims"][1]["edges"] == self.hist_2d.y_edges.tolist()
        assert hist["dims"][0]["length"] == self.hist_2d.num_bins
        assert hist["dims"][1]["length"] == self.hist_2d.num_bins
        assert hist["dims"][0]["type"] == np.float64
        assert hist["dims"][1]["type"] == np.float64
        assert np.array_equal(hist["data"], self.hist_2d.histogram)
