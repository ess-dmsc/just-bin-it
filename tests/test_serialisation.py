import pytest
import numpy as np
from histograms.histogram1d import Histogram1d
from histograms.histogram2d import Histogram2d
from endpoints.serialisation import serialise_hs00, deserialise_hs00


NUM_BINS = 5
X_RANGE = (0, 5)
Y_RANGE = (0, 10)
TOF_DATA = np.array([x for x in range(NUM_BINS)])
DET_DATA = np.array([x for x in range(NUM_BINS)])
PULSE_TIME = 12345


def _create_1d_histogrammer():
    histogrammer = Histogram1d("topic", NUM_BINS, X_RANGE)
    histogrammer.add_data(PULSE_TIME, TOF_DATA)
    return histogrammer


def _create_2d_histogrammer():
    histogrammer = Histogram2d("topic", NUM_BINS, X_RANGE, Y_RANGE)
    histogrammer.add_data(PULSE_TIME, TOF_DATA, DET_DATA)
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
        assert np.array_equal(hist["data"], self.hist_1d.data)

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
        assert np.array_equal(hist["data"], self.hist_2d.data)

    def test_serialises_hs00_message_with_info_field_filled_out_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        info_message = "info_message"
        buf = serialise_hs00(self.hist_1d, info_message)

        hist = deserialise_hs00(buf)
        assert hist["info"] == info_message
