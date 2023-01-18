import os

import pytest

import tests
from just_bin_it.endpoints.sources import HistogramSource
from tests.doubles.consumer import StubConsumer

INVALID_FB = b"this is an invalid fb message"


class TestHistogramSource:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Trick to get path of test data
        path = os.path.dirname(tests.__file__)
        with open(os.path.join(path, "example_hs00_fb.dat"), "rb") as f:
            self.valid_fb = f.read()

    def test_if_no_new_messages_then_no_data(self):
        mock_consumer = StubConsumer(["broker1"], ["topic1"])
        mock_consumer.add_messages([])
        hs = HistogramSource(mock_consumer)
        data = hs.get_new_data()
        assert len(data) == 0

    def test_if_five_new_messages_on_one_topic_then_data_has_five_items(self):
        mock_consumer = StubConsumer(["broker1"], ["topic1"])
        mock_consumer.add_messages([(0, 0, self.valid_fb)] * 5)
        hs = HistogramSource(mock_consumer)

        data = hs.get_new_data()
        _, _, message = data[0]

        assert len(data) == 5
        assert message["source"] == "just-bin-it"
        assert message["timestamp"] == 987_654_321
        assert message["current_shape"] == [50]
        assert len(message["data"]) == 50
        assert len(message["dim_metadata"]) == 1
        assert message["info"] == "hello"
        assert message["dim_metadata"][0]["length"] == 50
        assert len(message["dim_metadata"][0]["bin_boundaries"]) == 51
        assert message["dim_metadata"][0]["bin_boundaries"][0] == 0.0
        assert message["dim_metadata"][0]["bin_boundaries"][50] == 100_000_000.0

    def test_deserialising_invalid_fb_does_not_throw(self):
        mock_consumer = StubConsumer(["broker1"], ["topic1"])
        mock_consumer.add_messages([INVALID_FB])
        hs = HistogramSource(mock_consumer)

        hs.get_new_data()
