import numpy as np
import pytest
from just_bin_it.histograms.histogram1d import Histogram1d


class TestHistogram1d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.num_bins = 5
        self.range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins)])
        self.hist = Histogram1d("topic1", self.num_bins, self.range)

    def test_on_construction_histogram_is_initialised_empty(self):
        assert self.hist.x_edges is not None
        assert self.hist.shape == (self.num_bins,)
        assert len(self.hist.x_edges) == self.num_bins + 1
        assert self.hist.x_edges[0] == self.data[0]
        assert self.hist.x_edges[-1] == 5

    def test_adding_data_to_histogram_adds_data(self):
        self.hist.add_data(self.pulse_time, self.data)
        first_sum = self.hist.data.sum()

        # Add the data again
        self.hist.add_data(self.pulse_time, self.data)

        # Sum should be double
        assert self.hist.data.sum() == first_sum * 2

    def test_adding_data_outside_initial_bins_is_ignored(self):
        self.hist.add_data(self.pulse_time, self.data)
        first_sum = self.hist.data.sum()
        x_edges = self.hist.x_edges[:]

        # Add data that is outside the edges
        new_data = np.array([x + self.num_bins + 1 for x in range(self.num_bins)])
        self.hist.add_data(self.pulse_time, new_data)

        # Sum should not change
        assert self.hist.data.sum() == first_sum
        # Edges should not change
        assert np.array_equal(self.hist.x_edges, x_edges)

    def test_only_data_with_correct_source_is_added(self):
        hist = Histogram1d("topic1", self.num_bins, self.range, source="source1")

        hist.add_data(self.pulse_time, self.data, source="source1")
        hist.add_data(self.pulse_time, self.data, source="source1")
        hist.add_data(self.pulse_time, self.data, source="OTHER")

        assert hist.data.sum() == 10

    def test_clearing_histogram_data_clears_histogram(self):
        self.hist.add_data(self.pulse_time, self.data)

        self.hist.clear_data()

        assert self.hist.data.sum() == 0

    def test_after_clearing_histogram_can_add_data(self):
        self.hist.add_data(self.pulse_time, self.data)
        self.hist.clear_data()

        self.hist.add_data(self.pulse_time, self.data)

        assert self.hist.shape == (self.num_bins,)
        assert self.hist.data.sum() == 5

    def test_adding_empty_data_does_nothing(self):
        self.hist.add_data(self.pulse_time, [])

        assert self.hist.data.sum() == 0

    def test_histogram_keeps_track_of_last_pulse_time_processed(self):
        self.hist.add_data(1234, self.data)
        self.hist.add_data(1235, self.data)
        self.hist.add_data(1236, self.data)

        assert self.hist.last_pulse_time == 1236

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.identifier == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = Histogram1d("topic1", self.num_bins, self.range, identifier=example_id)
        assert hist.identifier == example_id

    def test_if_det_id_is_out_of_range_then_it_is_ignored(self):
        hist = Histogram1d("topic1", self.num_bins, self.range, (10, 20))
        tof_data = [0, 1, 2, 3, 4]
        det_data = [0, 10, 20, 30, 40]

        hist.add_data(12345, tof_data, det_data)

        assert hist.data.sum() == 2
