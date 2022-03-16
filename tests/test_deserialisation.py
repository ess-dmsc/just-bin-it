import os

import pytest
import numpy as np

import tests
from just_bin_it.endpoints.serialisation import (
    deserialise_ev42,
    deserialise_hs00,
    deserialise_hs01,
    get_schema,
    serialise_hs00,
    serialise_hs01,
)
from just_bin_it.exceptions import JustBinItException
from just_bin_it.histograms.histogram1d import Histogram1d


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
        data = deserialise_ev42(self.buf)

        assert data.message_id == 300
        assert data.pulse_time == 1_542_876_129_940_000_057
        assert data.source_name == "NeXus-Streamer"
        assert len(data.detector_id) == 794
        assert len(data.time_of_flight) == 794
        assert data.detector_id[0] == 99406
        assert data.time_of_flight[0] == 11_660_506

    def test_can_extract_the_schema_type(self):
        schema = get_schema(self.buf)

        assert schema == "ev42"

    def test_if_schema_is_incorrect_then_throws(self):
        new_buf = self.buf[:4] + b"na12" + self.buf[8:]

        with pytest.raises(Exception):
            deserialise_ev42(new_buf)


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
