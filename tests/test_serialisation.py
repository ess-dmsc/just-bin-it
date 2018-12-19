import pytest
import numpy as np
from histogrammer1d import Histogrammer1d
from histogrammer2d import Histogrammer2d
from deserialisation import serialise_hs00
from fbschemas.hs00.EventHistogram import EventHistogram
import fbschemas.hs00.ArrayFloat as ArrayFloat
from fbschemas.hs00.Array import Array


def _deserialise(buf):
    event_hist = EventHistogram.GetRootAsEventHistogram(buf, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins = event_hist.DimMetadata(i).BinBoundaries()

        # Get bins
        temp = ArrayFloat.ArrayFloat()
        temp.Init(bins.Bytes, bins.Pos)
        vals = temp.ValueAsNumpy()

        # Get type
        if event_hist.DimMetadata(i).BinBoundariesType() == Array.ArrayFloat:
            bin_type = np.float32
        else:
            raise TypeError("Could not determine bin boundaries type")

        info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": vals.tolist(),
            "type": bin_type,
        }
        dims.append(info)

    hist = {
        "source": event_hist.Source().decode("utf-8"),
        "shape": event_hist.CurrentShapeAsNumpy().tolist(),
        "dims": dims,
    }
    return hist


def _create_1d_histogrammer():
    histogrammer = Histogrammer1d((0, 5), 5)
    histogrammer.add_data(np.array([x for x in range(5)]))
    return histogrammer


def _create_2d_histogrammer():
    histogrammer = Histogrammer2d((0, 5), (0, 5), 5)
    data = np.array([x for x in range(5)])
    histogrammer.add_data(data, data)
    return histogrammer


class TestSerialisation:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.histogrammer1d = _create_1d_histogrammer()
        self.histogrammer2d = _create_2d_histogrammer()

    def test_serialises_hs00_message_correctly_for_1d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.histogrammer1d)

        hist = _deserialise(buf)
        assert "just-bin-it" == hist["source"]
        assert [5] == hist["shape"]
        assert [0.0, 1.0, 2.0, 3.0, 4.0, 5.0] == hist["dims"][0]["edges"]
        assert 5 == hist["dims"][0]["length"]
        assert np.float32 == hist["dims"][0]["type"]

    def test_serialises_hs00_message_correctly_for_2d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.histogrammer2d)

        hist = _deserialise(buf)
        assert "just-bin-it" == hist["source"]
        assert [5, 5] == hist["shape"]
        assert [0.0, 1.0, 2.0, 3.0, 4.0, 5.0] == hist["dims"][0]["edges"]
        assert [0.0, 1.0, 2.0, 3.0, 4.0, 5.0] == hist["dims"][1]["edges"]
        assert 5 == hist["dims"][0]["length"]
        assert 5 == hist["dims"][1]["length"]
        assert np.float32 == hist["dims"][0]["type"]
        assert np.float32 == hist["dims"][1]["type"]
