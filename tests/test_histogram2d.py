import pytest
import numpy as np
from histograms.histogram2d import Histogram2d


class TestHistogram2d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.num_bins = 5
        self.tof_range = (0, 10)
        self.det_range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins)])
        self.hist = Histogram2d("topic", self.num_bins, self.tof_range, self.det_range)

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.x_edges is None
        assert self.hist.y_edges is None

    def test_adding_data_to_uninitialised_histogram_initialises_it(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)

        assert self.hist.shape == (self.num_bins, self.num_bins)
        assert sum(sum(self.hist.data)) == 5
        # Edges is 1 more than the number of bins
        assert len(self.hist.x_edges) == self.num_bins + 1
        assert len(self.hist.y_edges) == self.num_bins + 1
        assert self.hist.x_edges[0] == self.data[0]
        assert self.hist.x_edges[-1] == 10
        assert self.hist.y_edges[-1] == 5

    def test_adding_data_to_initialised_histogram_new_data_is_added(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)
        first_sum = sum(sum(self.hist.data))

        # Add the data again
        self.hist.add_data(self.pulse_time, self.data, self.data)

        # Sum should be double
        assert sum(sum(self.hist.data)) == first_sum * 2

    def test_adding_data_outside_initial_bins_is_ignored(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)
        first_sum = sum(sum(self.hist.data))
        x_edges = self.hist.x_edges[:]
        y_edges = self.hist.y_edges[:]

        # Add data that is outside the edges
        new_data = np.array([x + self.num_bins + 1 for x in range(self.num_bins)])
        self.hist.add_data(self.pulse_time, new_data, new_data)

        # Sum should not change
        assert sum(sum(self.hist.data)) == first_sum
        # Edges should not change
        assert np.array_equal(self.hist.x_edges, x_edges)
        assert np.array_equal(self.hist.y_edges, y_edges)

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.id == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = Histogram2d(
            "topic1", self.num_bins, self.tof_range, self.det_range, id=example_id
        )
        assert hist.id == example_id