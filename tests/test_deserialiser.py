import pytest
import os
import tests
from deserialisation import deserialise_ev42


class TestDeserialiser:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Trick to get path of test data
        path = os.path.dirname(tests.__file__)
        with open(os.path.join(path, "example_fb.dat"), "rb") as f:
            self.buf = f.read()

    def test_deserialises_ev42_message_correctly(self):
        """
        Sanity check: checks the combination of libraries work as expected.
        """
        data = deserialise_ev42(self.buf)

        assert 300 == data["message_id"]
        assert 1_542_876_129_940_000_057 == data["pulse_time"]
        assert "NeXus-Streamer" == data["source"]
        assert 794 == len(data["det_ids"])
        assert 794 == len(data["tofs"])
        assert 99406 == data["det_ids"][0]
        assert 11_660_506 == data["tofs"][0]
