import numpy as np
import pytest

from just_bin_it.histograms.binned_data import BinnedData
from just_bin_it.histograms.histogram1d import Histogram1d

IRRELEVANT_TOPIC = "some-topic"
MATCHING_BINNED_DATA = BinnedData(
    np.array([0, 10, 20, 30]),
    np.array([[1, 2], [3, 4], [5, 6]]),
)
FRACTIONAL_BINNED_DATA = BinnedData(
    np.array([0, 10, 20, 30]),
    np.array([[10], [20], [30]]),
)
RANGED_BINNED_DATA = BinnedData(
    np.array([0, 10, 20, 30, 40]),
    np.array([[10], [20], [30], [40]]),
)
FILTERED_BINNED_DATA = BinnedData(
    np.array([0, 10, 20]),
    np.array([[1], [2]]),
)
SPATIAL_BINNED_DATA = BinnedData(
    np.array([0, 10, 20]),
    np.array([[1, 2, 3, 4], [5, 6, 7, 8]]),
)


class TestHistogram1dFunctionality:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.num_bins = 5
        self.range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins)])
        self.hist = Histogram1d(IRRELEVANT_TOPIC, self.num_bins, self.range)

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

    def test_adding_tof_data_counts_expected_bins(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 5, (0, 5))
        tof_data = [-1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 6]

        hist.add_data(self.pulse_time, tof_data)

        assert np.array_equal(hist.data, [2, 2, 2, 2, 3])

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
        hist = Histogram1d(
            IRRELEVANT_TOPIC, self.num_bins, self.range, source="source1"
        )

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
        hist = Histogram1d(
            IRRELEVANT_TOPIC, self.num_bins, self.range, identifier=example_id
        )
        assert hist.identifier == example_id

    def test_if_det_id_is_out_of_range_then_it_is_ignored(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, self.num_bins, self.range, (10, 20))
        tof_data = [0, 1, 2, 3, 4]
        det_data = [0, 10, 20, 30, 40]

        hist.add_data(12345, tof_data, det_data)

        assert hist.data.sum() == 2
        assert np.array_equal(hist.data, [0, 1, 1, 0, 0])

    def test_detector_range_filter_is_applied_before_tof_binning(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 5, (0, 5), (10, 20))
        tof_data = [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 2]
        det_data = [9, 10, 11, 20, 21, 15, 15, 19, 20, 10, 20, 5]

        hist.add_data(self.pulse_time, tof_data, det_data)

        assert np.array_equal(hist.data, [1, 2, 1, 2, 3])

    def test_matching_binned_data_bins_adds_counts_directly(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 3, (0, 30))

        hist.add_binned_data(123, MATCHING_BINNED_DATA)

        assert np.array_equal(hist.data, [3, 7, 11])
        assert np.issubdtype(hist.data.dtype, np.integer)

    def test_fractional_binned_data_rebinning_preserves_total_counts(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 2, (0, 30))

        hist.add_binned_data(123, FRACTIONAL_BINNED_DATA)

        assert np.array_equal(hist.data, [20, 40])
        assert hist.data.sum() == FRACTIONAL_BINNED_DATA.counts.sum()
        assert np.issubdtype(hist.data.dtype, np.floating)

    def test_binned_data_tof_range_is_respected(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 2, (10, 30))

        hist.add_binned_data(123, RANGED_BINNED_DATA)

        assert np.array_equal(hist.data, [20, 30])

    def test_binned_data_source_filtering_is_respected(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 2, (0, 20), source="source")

        hist.add_binned_data(123, FILTERED_BINNED_DATA, source="other")

        assert np.array_equal(hist.data, [0, 0])

    def test_binned_data_detector_range_filters_flattened_spatial_axes(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 2, (0, 20), det_range=(1, 2))

        hist.add_binned_data(123, SPATIAL_BINNED_DATA)

        assert np.array_equal(hist.data, [5, 13])

    @pytest.mark.parametrize("dtype", [np.int32, np.uint32, np.int64])
    @pytest.mark.parametrize(
        "bins, limits",
        [(1000, (0, 100_000_000)), (50, (0, 14)), (4, (5, 5)), ([1, 10], (0, 20))],
    )
    def test_integer_tof_counts_match_numpy_at_boundaries(self, dtype, bins, limits):
        edges = np.histogram_bin_edges([], bins=bins, range=limits)
        tofs = np.concatenate((edges, edges - 1, edges + 1)).astype(dtype)
        hist = Histogram1d(IRRELEVANT_TOPIC, bins, limits)

        hist.add_data(1, tofs)

        assert np.array_equal(hist.data, np.histogram(tofs, bins=bins, range=limits)[0])
        assert np.array_equal(hist.x_edges, edges)

    @pytest.mark.parametrize(
        "bins, limits",
        [
            (10, (0, 1)),
            (7, (0.1, 1.3)),
            (50, (0, 14)),
            (4, (5, 5)),
            ([1, 10, 10, 20], (0, 20)),
        ],
    )
    @pytest.mark.parametrize("dtype", [np.float16, np.float32, np.float64])
    def test_float_tof_counts_match_numpy_on_and_next_to_edges(
        self, dtype, bins, limits
    ):
        edges = np.histogram_bin_edges([], bins=bins, range=limits)
        tofs = np.concatenate(
            (
                edges,
                np.nextafter(edges, -np.inf),
                np.nextafter(edges, np.inf),
                [np.nan, -np.inf, np.inf],
            )
        ).astype(dtype)
        hist = Histogram1d(IRRELEVANT_TOPIC, bins, limits)

        hist.add_data(1, tofs)

        assert np.array_equal(hist.data, np.histogram(tofs, bins=bins, range=limits)[0])

    @pytest.mark.parametrize("det_range", [(10, 20), (15, 15)])
    def test_detector_filter_and_tof_edges_are_independent(self, det_range):
        tofs = np.array([-1, 0, 7, 7, 14, 15, 7, 7, 7], dtype=np.int32)
        dets = np.array([15, 10, 15, 20, 15, 15, 9, 21, 15], dtype=np.int32)
        hist = Histogram1d(IRRELEVANT_TOPIC, 50, (0, 14), det_range)
        expected = np.histogram2d(tofs, dets, bins=50, range=((0, 14), det_range))[
            0
        ].sum(axis=1)

        hist.add_data(1, tofs, dets)

        assert np.array_equal(hist.data, expected)
        assert hist.data.dtype == np.float64

    def test_equal_detector_limits_use_expanded_numpy_range(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 5, (0, 10), (15, 15))
        tofs = np.array([1, 1, 1, 1, 1])
        dets = np.array([14.4, 14.5, 15, 15.5, 15.6])
        expected = np.histogram2d(tofs, dets, bins=5, range=((0, 10), (15, 15)))[0]

        hist.add_data(self.pulse_time, tofs, dets)

        assert np.array_equal(hist.data, expected.sum(axis=1))

    @pytest.mark.parametrize("dtype", [np.int32, np.uint32, np.float64])
    def test_random_batches_accumulate_like_numpy(self, dtype):
        rng = np.random.default_rng(5095)
        hist = Histogram1d(IRRELEVANT_TOPIC, 31, (100, 999), (10, 90))
        expected = np.zeros(hist.shape)
        for pulse_time in range(3):
            tofs = rng.uniform(0, 1100, 2000).astype(dtype)
            dets = rng.uniform(0, 100, 2000).astype(dtype)

            hist.add_data(pulse_time, tofs, dets)

            expected += np.histogram(
                tofs[(dets >= 10) & (dets <= 90)], bins=31, range=(100, 999)
            )[0]
        assert np.array_equal(hist.data, expected)

    def test_fractional_tof_data_can_be_followed_by_events_and_reset(self):
        hist = Histogram1d(IRRELEVANT_TOPIC, 2, (0, 3))
        hist.add_binned_data(
            1, BinnedData(np.array([0, 1, 2, 3]), np.array([[1], [1], [1]]))
        )
        hist.add_data(2, [0, 1.5, 3])

        assert np.array_equal(hist.data, [2.5, 3.5])
        hist.clear_data()
        hist.add_data(3, [0, 1.5, 3])
        assert np.array_equal(hist.data, [1, 2])
        assert hist.data.dtype == np.int64
