import pytest
import numpy as np
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d
from endpoints.serialisation import serialise_hs00
from fbschemas.hs00.EventHistogram import EventHistogram
import fbschemas.hs00.ArrayDouble as ArrayDouble
from fbschemas.hs00.Array import Array


NUM_BINS = 5
X_RANGE = (0, 5)
Y_RANGE = (0, 10)
TOF_DATA = np.array([x for x in range(NUM_BINS)])
DET_DATA = np.array([x for x in range(NUM_BINS)])


def _deserialise(buf):
    """
    Convert flatbuffer into a histogram.

    Helper function for unit tests - do not use anywhere else.

    :param buf:
    :return: dict of histogram information
    """
    event_hist = EventHistogram.GetRootAsEventHistogram(buf, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_fb = event_hist.DimMetadata(i).BinBoundaries()

        # Get bins
        temp = ArrayDouble.ArrayDouble()
        temp.Init(bins_fb.Bytes, bins_fb.Pos)
        bins = temp.ValueAsNumpy()

        # Get type
        if event_hist.DimMetadata(i).BinBoundariesType() == Array.ArrayDouble:
            bin_type = np.float64
        else:
            raise TypeError("Type of the bin boundaries is incorrect")

        info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": bins.tolist(),
            "type": bin_type,
        }
        dims.append(info)

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError("Type of the data array is incorrect")

    data_fb = event_hist.Data()
    temp = ArrayDouble.ArrayDouble()
    temp.Init(data_fb.Bytes, data_fb.Pos)
    data = temp.ValueAsNumpy()
    shape = event_hist.CurrentShapeAsNumpy().tolist()

    hist = {
        "source": event_hist.Source().decode("utf-8"),
        "shape": shape,
        "dims": dims,
        "data": data.reshape(shape),
    }
    return hist


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

        hist = _deserialise(buf)
        assert "just-bin-it" == hist["source"]
        assert [self.hist_1d.num_bins] == hist["shape"]
        assert self.hist_1d.x_edges.tolist() == hist["dims"][0]["edges"]
        assert self.hist_1d.num_bins == hist["dims"][0]["length"]
        assert np.float64 == hist["dims"][0]["type"]
        assert np.array_equal(self.hist_1d.histogram, hist["data"])

    def test_serialises_hs00_message_correctly_for_2d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.hist_2d)

        hist = _deserialise(buf)
        assert "just-bin-it" == hist["source"]
        assert [self.hist_2d.num_bins, self.hist_2d.num_bins] == hist["shape"]
        assert self.hist_2d.x_edges.tolist() == hist["dims"][0]["edges"]
        assert self.hist_2d.y_edges.tolist() == hist["dims"][1]["edges"]
        assert self.hist_2d.num_bins == hist["dims"][0]["length"]
        assert self.hist_2d.num_bins == hist["dims"][1]["length"]
        assert np.float64 == hist["dims"][0]["type"]
        assert np.float64 == hist["dims"][1]["type"]
        assert np.array_equal(self.hist_2d.histogram, hist["data"])
