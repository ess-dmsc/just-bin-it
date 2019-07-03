import pytest
import os
import tests
import numpy as np
from endpoints.serialisation import deserialise_ev42, deserialise_hs00, get_schema


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

        assert data["message_id"] == 300
        assert data["pulse_time"] == 1_542_876_129_940_000_057
        assert data["source"] == "NeXus-Streamer"
        assert len(data["det_ids"]) == 794
        assert len(data["tofs"]) == 794
        assert data["det_ids"][0] == 99406
        assert data["tofs"][0] == 11_660_506

    def test_can_extract_the_schema_type(self):
        schema = get_schema(self.buf)

        assert schema == "ev42"

    def test_if_schema_is_incorrect_then_throws(self):
        new_buf = self.buf[:4] + b"na12" + self.buf[8:]

        with pytest.raises(Exception):
            deserialise_ev42(new_buf)


class TestDeserialisationHs00:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Trick to get path of test data
        path = os.path.dirname(tests.__file__)
        with open(os.path.join(path, "example_hs00_fb.dat"), "rb") as f:
            self.buf = f.read()

    def test_deserialises_hs00_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        data = deserialise_hs00(self.buf)

        assert data["source"] == "just-bin-it"
        assert data["timestamp"] == 987_654_321
        assert data["shape"] == [50]
        assert len(data["data"]) == 50
        assert len(data["dims"]) == 1
        assert data["info"] == "hello"

        assert data["dims"][0]["length"] == 50
        assert data["dims"][0]["type"] == np.float64
        assert len(data["dims"][0]["edges"]) == 51
        assert data["dims"][0]["edges"][0] == 0.0
        assert data["dims"][0]["edges"][50] == 100_000_000.0

    def test_if_schema_is_incorrect_then_throws(self):
        new_buf = self.buf[:4] + b"na12" + self.buf[8:]

        with pytest.raises(Exception):
            deserialise_hs00(new_buf)
