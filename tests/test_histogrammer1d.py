import pytest
import numpy as np
from histograms.histogrammer1d import Histogrammer1d


class TestHistogrammer1d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.num_bins = 5
        self.range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins)])
        self.hist = Histogrammer1d("topic1", self.num_bins, self.range)

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.histogram is None
        assert self.hist.x_edges is None

    def test_adding_data_to_uninitialised_histogram_initialises_it(self):
        self.hist.add_data(self.pulse_time, self.data)

        assert self.hist.histogram is not None
        assert self.hist.histogram.shape == (self.num_bins,)
        assert sum(self.hist.histogram) == 5
        # Edges is 1 more than the number of bins
        assert len(self.hist.x_edges) == self.num_bins + 1
        assert self.hist.x_edges[0] == self.data[0]
        assert self.hist.x_edges[-1] == 5

    def test_adding_data_to_initialised_histogram_new_data_is_added(self):
        self.hist.add_data(self.pulse_time, self.data)
        first_sum = sum(self.hist.histogram)

        # Add the data again
        self.hist.add_data(self.pulse_time, self.data)

        # Sum should be double
        assert sum(self.hist.histogram) == first_sum * 2

    def test_adding_data_outside_initial_bins_is_ignored(self):
        self.hist.add_data(self.pulse_time, self.data)
        first_sum = sum(self.hist.histogram)
        x_edges = self.hist.x_edges[:]

        # Add data that is outside the edges
        new_data = np.array([x + self.num_bins + 1 for x in range(self.num_bins)])
        self.hist.add_data(self.pulse_time, new_data)

        # Sum should not change
        assert sum(self.hist.histogram) == first_sum
        # Edges should not change
        assert np.array_equal(self.hist.x_edges, x_edges)

    def test_adding_preprocessing_step_affects_data_histogrammed(self):
        # Only add data if pulse time is even.
        def _preprocess_step(pulse_time, x):
            if pulse_time % 2 == 0:
                return x
            else:
                return np.zeros(0)

        self.hist = Histogrammer1d(
            "topic1", self.num_bins, self.range, _preprocess_step
        )
        self.hist.add_data(3, self.data)

        # As pulse time is odd no data should be added.
        assert sum(self.hist.histogram) == 0

    def test_throwing_preprocessing_step_is_handled(self):
        def _preprocess(pulse_time, x):
            raise Exception("Preprocessing failed.")

        self.hist = Histogrammer1d("topic1", self.num_bins, self.range, _preprocess)
        self.hist.add_data(self.pulse_time, self.data)

    def test_if_no_tof_range_given_then_sets_upper_limit_to_max_value(self):
        self.hist = Histogrammer1d("topic1", self.num_bins)

        self.hist.add_data(self.pulse_time, self.data)

        assert self.hist.tof_range[0] == 0
        assert self.hist.tof_range[1] == max(self.data)
