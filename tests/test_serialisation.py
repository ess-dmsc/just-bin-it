import numpy as np
import pytest

from just_bin_it.endpoints.serialisation import (
    deserialise_da00,
    deserialise_ev42,
    deserialise_hs00,
    deserialise_hs01,
    serialise_da00,
    serialise_ev42,
    serialise_hs00,
    serialise_hs01,
)
from just_bin_it.histograms.binned_data import BinnedData
from just_bin_it.histograms.histogram1d import Histogram1d
from just_bin_it.histograms.histogram2d import Histogram2d
from just_bin_it.histograms.histogram2d_map import DetHistogram
from just_bin_it.histograms.histogram2d_roi import RoiHistogram

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


@pytest.mark.parametrize(
    "serialise, deserialise",
    [(serialise_hs00, deserialise_hs00), (serialise_hs01, deserialise_hs01)],
)
class TestHistogramSerialisation:
    def test_detector_map_preserves_mixed_counts_and_geometry(
        self, serialise, deserialise
    ):
        hist = DetHistogram("topic", (10, 999), 3, 2)
        hist.add_data(PULSE_TIME, [], [10, 10, 12, 13, 15])
        hist.add_binned_data(
            PULSE_TIME,
            BinnedData(
                np.array([0, 1, 2]),
                np.array([[0.5, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6]]),
            ),
        )

        result = deserialise(serialise(hist, PULSE_TIME))

        assert result["timestamp"] == PULSE_TIME
        assert result["current_shape"] == [3, 2]
        assert result["data"].dtype == np.float64
        assert np.array_equal(result["data"], [[3.5, 8], [3, 9], [6, 12]])
        assert np.array_equal(result["dim_metadata"][0]["bin_boundaries"], [0, 1, 2, 3])
        assert np.array_equal(result["dim_metadata"][1]["bin_boundaries"], [0, 1, 2])
        assert result["dim_metadata"][0]["length"] == 3
        assert result["dim_metadata"][1]["length"] == 2
        assert result["data"].sum() == hist.counts_sum()

    def test_roi_preserves_counts_and_geometry(self, serialise, deserialise):
        hist = RoiHistogram("topic", [10, 20], 3)
        hist.add_data(PULSE_TIME, [], [9, 10, 10, 12, 13, 19, 20, 22, 23])

        result = deserialise(serialise(hist, PULSE_TIME))

        assert result["current_shape"] == [3, 2]
        assert result["data"].dtype == np.float64
        assert np.array_equal(result["data"], [[2, 1], [0, 0], [1, 1]])
        assert np.array_equal(result["dim_metadata"][0]["bin_boundaries"], hist.x_edges)
        assert np.array_equal(result["dim_metadata"][1]["bin_boundaries"], hist.y_edges)
        assert result["data"].sum() == hist.counts_sum()

    @pytest.mark.parametrize(
        "det_range, dtype", [(None, np.integer), ((10, 20), np.float64)]
    )
    def test_tof_preserves_count_dtype(self, serialise, deserialise, det_range, dtype):
        hist = Histogram1d("topic", 5, (0, 5), det_range)
        hist.add_data(PULSE_TIME, [0, 1, 5], [10, 15, 20])

        result = deserialise(serialise(hist, PULSE_TIME))

        # hs00 uses uint64 for integer counts; hs01 retains int64.
        assert np.issubdtype(result["data"].dtype, dtype)
        assert result["data"].dtype.itemsize == 8
        assert np.array_equal(result["data"], [1, 1, 0, 0, 1])


class TestSerialisationEv42:
    def test_serialises_ev42_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        source = "just-bin-it"
        pulse_time = 1234567890000000000
        tofs = [1, 2, 3, 4, 5]
        dets = [10, 20, 30, 40, 50]
        buf = serialise_ev42(source, 123456, pulse_time, tofs, dets)

        info = deserialise_ev42(buf)
        assert info[0] == source
        assert info[1] == pulse_time
        assert np.array_equal(info[2], tofs)
        assert np.array_equal(info[3], dets)


class TestSerialisationDa00:
    def test_serialises_da00_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        source = "just-bin-it"
        timestamp = 1234567890000000000
        buf = serialise_da00(
            source,
            timestamp,
            [
                {"name": "signal", "data": [1, 2], "axes": ["frame_time"]},
                {
                    "name": "frame_time",
                    "data": [0, 10, 20],
                    "axes": ["frame_time"],
                    "unit": "ns",
                },
            ],
        )

        actual_source, actual_timestamp, binned_data, _ = deserialise_da00(buf)

        assert actual_source == source
        assert actual_timestamp == timestamp
        assert np.array_equal(binned_data.tof_edges, [0, 10, 20])
        assert np.array_equal(binned_data.counts, [[1], [2]])
