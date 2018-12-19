import pytest
import numpy as np
from histogrammer1d import Histogrammer1d


class TestHistogrammer1d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.num_bins = 5
        self.range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins)])

    def test_on_construction_histogram_is_uninitialised(self):
        h = Histogrammer1d(self.range, self.num_bins)
        assert h.histogram is None
        assert h.x_edges is None

    def test_adding_data_to_uninitialised_histogram_initialises_it(self):
        h = Histogrammer1d(self.range, self.num_bins)
        h.add_data(self.data)

        assert h.histogram is not None
        assert (self.num_bins,) == h.histogram.shape
        assert 5 == sum(h.histogram)
        # Edges is 1 more than the number of bins
        assert self.num_bins + 1 == len(h.x_edges)
        assert self.data[0] == h.x_edges[0]
        assert 5 == h.x_edges[-1]

    def test_adding_data_to_initialised_histogram_new_data_is_added(self):
        h = Histogrammer1d(self.range, self.num_bins)
        h.add_data(self.data)
        first_sum = sum(h.histogram)

        # Add the data again
        h.add_data(self.data)

        # Sum should be double
        assert first_sum * 2 == sum(h.histogram)

    def test_adding_data_outside_initial_bins_is_ignored(self):
        h = Histogrammer1d(self.range, self.num_bins)
        h.add_data(self.data)
        first_sum = sum(h.histogram)
        x_edges = h.x_edges[:]

        # Add data that is outside the edges
        new_data = np.array([x + self.num_bins + 1 for x in range(self.num_bins)])
        h.add_data(new_data)

        # Sum should not change
        assert first_sum == sum(h.histogram)
        # Edges should not change
        assert np.array_equal(x_edges, h.x_edges)
