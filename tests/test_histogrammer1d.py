import pytest
import numpy as np
from histogrammer1d import Histogrammer1d


class TestHistogrammer1d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.num_bins = 5
        self.range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins)])
        self.hist = Histogrammer1d(self.range, self.num_bins, "topic1")

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.histogram is None
        assert self.hist.x_edges is None

    def test_adding_data_to_uninitialised_histogram_initialises_it(self):
        self.hist.add_data(self.data)

        assert self.hist.histogram is not None
        assert (self.num_bins,) == self.hist.histogram.shape
        assert 5 == sum(self.hist.histogram)
        # Edges is 1 more than the number of bins
        assert self.num_bins + 1 == len(self.hist.x_edges)
        assert self.data[0] == self.hist.x_edges[0]
        assert 5 == self.hist.x_edges[-1]

    def test_adding_data_to_initialised_histogram_new_data_is_added(self):
        self.hist.add_data(self.data)
        first_sum = sum(self.hist.histogram)

        # Add the data again
        self.hist.add_data(self.data)

        # Sum should be double
        assert first_sum * 2 == sum(self.hist.histogram)

    def test_adding_data_outside_initial_bins_is_ignored(self):
        self.hist.add_data(self.data)
        first_sum = sum(self.hist.histogram)
        x_edges = self.hist.x_edges[:]

        # Add data that is outside the edges
        new_data = np.array([x + self.num_bins + 1 for x in range(self.num_bins)])
        self.hist.add_data(new_data)

        # Sum should not change
        assert first_sum == sum(self.hist.histogram)
        # Edges should not change
        assert np.array_equal(x_edges, self.hist.x_edges)
