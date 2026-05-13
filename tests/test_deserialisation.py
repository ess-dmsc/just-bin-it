import os

import numpy as np
import pytest

import tests
from just_bin_it.endpoints.serialisation import (
    deserialise_da00,
    deserialise_ev42,
    deserialise_ev44,
    deserialise_hs00,
    deserialise_hs01,
    get_schema,
    serialise_da00,
    serialise_hs00,
    serialise_hs01,
)
from just_bin_it.exceptions import JustBinItException
from just_bin_it.histograms.histogram1d import Histogram1d


def make_da00_buffer(
    signal_data,
    signal_axes,
    frame_time,
    unit="ns",
    source="source",
    timestamp=123,
    include_signal=True,
    include_frame_time=True,
):
    variables = []
    if include_signal:
        variables.append({"name": "signal", "data": signal_data, "axes": signal_axes})
    if include_frame_time:
        variables.append(
            {
                "name": "frame_time",
                "data": frame_time,
                "axes": ["frame_time"],
                "unit": unit,
            }
        )
    return serialise_da00(source, timestamp, variables)


class TestDeserialisationEv42:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Trick to get path of test data
        path = os.path.dirname(tests.__file__)
        with open(os.path.join(path, "example_ev42_fb.dat"), "rb") as f:
            self.buf = f.read()

    def test_deserialises_ev42_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        source, pulse_time, tofs, det_ids = deserialise_ev42(self.buf)

        assert pulse_time == 1_542_876_129_940_000_057
        assert source == "NeXus-Streamer"
        assert len(det_ids) == 794
        assert len(tofs) == 794
        assert det_ids[0] == 99406
        assert tofs[0] == 11_660_506

    def test_can_extract_the_schema_type(self):
        schema = get_schema(self.buf)

        assert schema == "ev42"

    def test_if_schema_is_incorrect_then_throws(self):
        new_buf = self.buf[:4] + b"na12" + self.buf[8:]

        with pytest.raises(Exception):
            deserialise_ev42(new_buf)


class TestDeserialisationEv44:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Trick to get path of test data
        path = os.path.dirname(tests.__file__)
        with open(os.path.join(path, "example_ev44_fb.dat"), "rb") as f:
            self.buf = f.read()

    def test_deserialises_ev44_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        source, pulse_time, tofs, det_ids = deserialise_ev44(self.buf)

        assert pulse_time == 1_673_942_576_096_540_000
        assert source == "grace"
        assert len(det_ids) == 1577
        assert len(tofs) == 1577
        assert det_ids[0] == 128880
        assert tofs[0] == 128880

    def test_can_extract_the_schema_type(self):
        schema = get_schema(self.buf)

        assert schema == "ev44"

    def test_if_schema_is_incorrect_then_throws(self):
        new_buf = self.buf[:4] + b"na12" + self.buf[8:]

        with pytest.raises(Exception):
            deserialise_ev44(new_buf)


class TestDeserialisationDa00:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Trick to get path of test data
        path = os.path.dirname(tests.__file__)
        with open(os.path.join(path, "example_da00_fb.dat"), "rb") as f:
            self.buf = f.read()

    def test_deserialises_da00_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        source, pulse_time, binned_data, _ = deserialise_da00(self.buf)

        assert pulse_time == 1_700_000_000_000_000_000
        assert source == "da00-source"
        assert np.array_equal(binned_data.tof_edges, [0, 10_000, 20_000, 30_000])
        assert np.array_equal(binned_data.counts, [[1, 4], [2, 5], [3, 6]])

    def test_can_extract_the_schema_type(self):
        schema = get_schema(self.buf)

        assert schema == "da00"

    def test_if_schema_is_incorrect_then_throws(self):
        new_buf = self.buf[:4] + b"na12" + self.buf[8:]

        with pytest.raises(Exception):
            deserialise_da00(new_buf)

    def test_extracts_signal_frame_time_source_and_timestamp(self):
        buf = make_da00_buffer([1, 2, 3], ["frame_time"], [0, 10, 20, 30], "us")

        source, timestamp, binned_data, _ = deserialise_da00(buf)

        assert source == "source"
        assert timestamp == 123
        assert np.array_equal(binned_data.tof_edges, [0, 10_000, 20_000, 30_000])
        assert np.array_equal(binned_data.counts, [[1], [2], [3]])

    def test_accepts_upper_edge_shorthand(self):
        buf = make_da00_buffer([1, 2, 3], ["frame_time"], [10, 20, 30])

        _, _, binned_data, _ = deserialise_da00(buf)

        assert np.array_equal(binned_data.tof_edges, [0, 10, 20, 30])

    @pytest.mark.parametrize(
        "unit,factor",
        [
            ("ns", 1),
            ("us", 1_000),
            ("usec", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
        ],
    )
    def test_converts_supported_frame_time_units(self, unit, factor):
        buf = make_da00_buffer([1, 2], ["frame_time"], [0, 2, 4], unit)

        _, _, binned_data, _ = deserialise_da00(buf)

        assert np.array_equal(binned_data.tof_edges, np.array([0, 2, 4]) * factor)

    def test_handles_1d_spatial_signal_shape(self):
        signal = np.array([[1, 2, 3], [4, 5, 6]])
        buf = make_da00_buffer(signal, ["pixel", "frame_time"], [0, 10, 20, 30])

        _, _, binned_data, _ = deserialise_da00(buf)

        assert np.array_equal(binned_data.counts, signal.T)

    def test_handles_2d_spatial_signal_shape(self):
        signal = np.arange(12).reshape(2, 3, 2)
        buf = make_da00_buffer(signal, ["x", "frame_time", "y"], [0, 10, 20, 30])

        _, _, binned_data, _ = deserialise_da00(buf)

        assert np.array_equal(
            binned_data.counts, np.moveaxis(signal, 1, 0).reshape(3, 4)
        )

    def test_rejects_unknown_frame_time_units(self):
        buf = make_da00_buffer([1, 2], ["frame_time"], [0, 1, 2], "::unit::")

        with pytest.raises(JustBinItException):
            deserialise_da00(buf)

    def test_rejects_missing_signal(self):
        buf = make_da00_buffer([], [], [0, 1], include_signal=False)

        with pytest.raises(JustBinItException):
            deserialise_da00(buf)

    def test_rejects_missing_frame_time(self):
        buf = make_da00_buffer([1], ["frame_time"], [], include_frame_time=False)

        with pytest.raises(JustBinItException):
            deserialise_da00(buf)

    def test_rejects_missing_frame_time_axis(self):
        buf = make_da00_buffer([1], ["tof"], [0, 1])

        with pytest.raises(JustBinItException):
            deserialise_da00(buf)

    def test_rejects_non_monotonic_frame_time_edges(self):
        buf = make_da00_buffer([1, 2], ["frame_time"], [0, 10, 5])

        with pytest.raises(JustBinItException):
            deserialise_da00(buf)

    def test_rejects_incompatible_signal_shape(self):
        buf = make_da00_buffer([[1, 2], [3, 4]], ["frame_time"], [0, 1, 2])

        with pytest.raises(JustBinItException):
            deserialise_da00(buf)


class TestSerialisationHs00:
    """
    Sanity check: checks the combination of libraries work as expected.
    """

    def test_if_schema_is_incorrect_then_throws(self):
        buf = self._create_buffer()
        new_buf = buf[:4] + b"na12" + buf[8:]

        with pytest.raises(JustBinItException):
            deserialise_hs00(new_buf)

    def test_round_trip(self):
        buf = self._create_buffer()
        result = deserialise_hs00(buf)

        assert result["source"] == "just-bin-it"
        assert result["timestamp"] == 123
        assert result["current_shape"] == [10]
        assert result["dim_metadata"][0]["length"] == 10
        assert np.array_equal(
            result["dim_metadata"][0]["bin_boundaries"],
            [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        )
        assert np.array_equal(result["data"], [2, 0, 0, 0, 0, 1, 0, 1, 0, 1])

    def _create_buffer(self):
        h1d = Histogram1d(
            "::topic::",
            num_bins=10,
            tof_range=(0, 100),
            det_range=(0, 200),
            source="",
            identifier="::id::",
        )
        h1d.add_data(123, [0, 1, 50, 75, 99], [0, 100, 150, 175, 199])
        buf = serialise_hs00(h1d, 123)
        return buf


class TestSerialisationHs01:
    """
    Sanity check: checks the combination of libraries work as expected.
    """

    def test_if_schema_is_incorrect_then_throws(self):
        buf = self._create_buffer()
        new_buf = buf[:4] + b"na12" + buf[8:]

        with pytest.raises(JustBinItException):
            deserialise_hs01(new_buf)

    def test_round_trip(self):
        buf = self._create_buffer()
        result = deserialise_hs01(buf)

        assert result["source"] == "just-bin-it"
        assert result["timestamp"] == 123
        assert result["current_shape"] == [10]
        assert result["dim_metadata"][0]["length"] == 10
        assert np.array_equal(
            result["dim_metadata"][0]["bin_boundaries"],
            [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        )
        assert np.array_equal(result["data"], [2, 0, 0, 0, 0, 1, 0, 1, 0, 1])

    def _create_buffer(self):
        h1d = Histogram1d(
            "::topic::",
            num_bins=10,
            tof_range=(0, 100),
            det_range=(0, 200),
            source="",
            identifier="::id::",
        )
        h1d.add_data(123, [0, 1, 50, 75, 99], [0, 100, 150, 175, 199])
        buf = serialise_hs01(h1d, 123)
        return buf
