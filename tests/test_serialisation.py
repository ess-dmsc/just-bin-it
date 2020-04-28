import numpy as np
import pytest
from just_bin_it.endpoints.serialisation import (
    deserialise_hs00,
    serialise_hs00,
    deserialise_ev42,
    serialise_ev42,
)
from just_bin_it.histograms.histogram1d import Histogram1d
from just_bin_it.histograms.histogram2d import Histogram2d


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


class TestSerialisationHs00:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.hist_1d = _create_1d_histogrammer()
        self.hist_2d = _create_2d_histogrammer()

    def test_serialises_hs00_message_correctly_for_1d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        timestamp = 1234567890
        buf = serialise_hs00(self.hist_1d, timestamp)

        hist = deserialise_hs00(buf)
        assert hist["source"] == "just-bin-it"
        assert hist["timestamp"] == timestamp
        assert hist["current_shape"] == [self.hist_1d.num_bins]
        assert np.array_equal(
            hist["dim_metadata"][0]["bin_boundaries"], self.hist_1d.x_edges.tolist()
        )
        assert hist["dim_metadata"][0]["length"] == self.hist_1d.num_bins
        assert np.array_equal(hist["data"], self.hist_1d.data)

    def test_if_timestamp_not_supplied_then_it_is_zero(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.hist_1d)

        hist = deserialise_hs00(buf)
        assert hist["source"] == "just-bin-it"
        assert hist["timestamp"] == 0
        assert hist["current_shape"] == [self.hist_1d.num_bins]
        assert np.array_equal(
            hist["dim_metadata"][0]["bin_boundaries"], self.hist_1d.x_edges.tolist()
        )
        assert hist["dim_metadata"][0]["length"] == self.hist_1d.num_bins
        assert np.array_equal(hist["data"], self.hist_1d.data)

    def test_serialises_hs00_message_correctly_for_2d(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        buf = serialise_hs00(self.hist_2d)

        hist = deserialise_hs00(buf)
        assert hist["source"] == "just-bin-it"
        assert hist["current_shape"] == [self.hist_2d.num_bins, self.hist_2d.num_bins]
        assert np.array_equal(
            hist["dim_metadata"][0]["bin_boundaries"], self.hist_2d.x_edges.tolist()
        )
        assert np.array_equal(
            hist["dim_metadata"][1]["bin_boundaries"], self.hist_2d.y_edges.tolist()
        )
        assert hist["dim_metadata"][0]["length"] == self.hist_2d.num_bins
        assert hist["dim_metadata"][1]["length"] == self.hist_2d.num_bins
        assert np.array_equal(hist["data"], self.hist_2d.data)

    def test_serialises_hs00_message_with_info_field_filled_out_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        info_message = "info_message"
        buf = serialise_hs00(self.hist_1d, info_message=info_message)

        hist = deserialise_hs00(buf)
        assert hist["info"] == info_message


class TestSerialisationEv42:
    def test_serialises_ev42_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        source = "just-bin-it"
        message_id = 123456
        pulse_time = 1234567890000000000
        tofs = [1, 2, 3, 4, 5]
        dets = [10, 20, 30, 40, 50]
        buf = serialise_ev42(source, message_id, pulse_time, tofs, dets)

        info = deserialise_ev42(buf)
        assert info.source_name == source
        assert info.message_id == message_id
        assert info.pulse_time == pulse_time
        assert len(info.time_of_flight) == len(tofs)
        assert len(info.detector_id) == len(dets)
        assert np.array_equal(info.time_of_flight, tofs)
        assert np.array_equal(info.detector_id, dets)
