import numpy as np
import pytest

from just_bin_it.histograms.histogram2d import Histogram2d

IRRELEVANT_TOPIC = "some-topic"


class TestHistogram2dFunctionality:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.num_bins = (5, 10)
        self.tof_range = (0, 10)
        self.det_range = (0, 5)
        self.data = np.array([x for x in range(self.num_bins[0])])
        self.hist = Histogram2d("topic", self.num_bins, self.tof_range, self.det_range)

    def test_if_single_value_for_num_bins_then_value_used_for_both_x_and_y(self):
        num_bins = 5
        hist = Histogram2d("topic", num_bins, self.tof_range, self.det_range)
        assert len(hist.x_edges) == num_bins + 1
        assert len(hist.y_edges) == num_bins + 1
        assert hist.shape == (num_bins, num_bins)

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.x_edges is not None
        assert self.hist.y_edges is not None
        assert self.hist.shape == self.num_bins
        assert len(self.hist.x_edges) == self.num_bins[0] + 1
        assert len(self.hist.y_edges) == self.num_bins[1] + 1
        assert self.hist.x_edges[0] == self.data[0]
        assert self.hist.x_edges[-1] == 10
        assert self.hist.y_edges[0] == self.data[0]
        assert self.hist.y_edges[-1] == 5
        assert self.hist.data.sum() == 0

    def test_adding_data_to_initialised_histogram_new_data_is_added(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)
        first_sum = self.hist.data.sum()

        # Add the data again
        self.hist.add_data(self.pulse_time, self.data, self.data)

        # Sum should be double
        assert self.hist.data.sum() == first_sum * 2

    def test_adding_tof_and_detector_data_counts_expected_bins(self):
        hist = Histogram2d("topic", (2, 3), (0, 2), (10, 13))
        tof_data = [-1, 0, 0, 0, 1, 1, 2, 3]
        det_data = [10, 10, 10, 11, 12, 12, 13, 12]

        hist.add_data(self.pulse_time, tof_data, det_data)

        assert np.array_equal(hist.data, [[2, 1, 0], [0, 0, 3]])

    def test_adding_data_outside_initial_bins_is_ignored(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)
        first_sum = self.hist.data.sum()
        x_edges = self.hist.x_edges[:]
        y_edges = self.hist.y_edges[:]

        # Add data that is outside the edges
        new_data = np.array([x + self.num_bins[0] + 1 for x in range(self.num_bins[0])])
        self.hist.add_data(self.pulse_time, new_data, new_data)

        # Sum should not change
        assert self.hist.data.sum() == first_sum
        # Edges should not change
        assert np.array_equal(self.hist.x_edges, x_edges)
        assert np.array_equal(self.hist.y_edges, y_edges)

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.identifier == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = Histogram2d(
            "topic1",
            self.num_bins,
            self.tof_range,
            self.det_range,
            identifier=example_id,
        )
        assert hist.identifier == example_id

    def test_only_data_with_correct_source_is_added(self):
        hist = Histogram2d(
            "topic", self.num_bins, self.tof_range, self.det_range, source="source1"
        )

        hist.add_data(self.pulse_time, self.data, self.data, source="source1")
        hist.add_data(self.pulse_time, self.data, self.data, source="source1")
        hist.add_data(self.pulse_time, self.data, self.data, source="OTHER")

        assert hist.data.sum() == 10

    def test_clearing_histogram_data_clears_histogram(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)

        self.hist.clear_data()

        assert self.hist.data.sum() == 0

    def test_clearing_histogram_preserves_previously_returned_data(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)
        data = self.hist.data

        self.hist.clear_data()

        assert data.sum() == len(self.data)
        assert self.hist.counts_sum() == 0

    def test_after_clearing_histogram_can_add_data(self):
        self.hist.add_data(self.pulse_time, self.data, self.data)
        self.hist.clear_data()

        self.hist.add_data(self.pulse_time, self.data, self.data)

        assert self.hist.shape == self.num_bins
        assert self.hist.data.sum() == 5

    def test_adding_empty_data_does_nothing(self):
        self.hist.add_data(self.pulse_time, [], [])

        assert self.hist.data.sum() == 0

    def test_histogram_keeps_track_of_last_pulse_time_processed(self):
        self.hist.add_data(1234, self.data, self.data)
        self.hist.add_data(1235, self.data, self.data)
        self.hist.add_data(1236, self.data, self.data)

        assert self.hist.last_pulse_time == 1236

    @pytest.mark.parametrize("dtype", [np.int32, np.uint32, np.float64])
    def test_random_batches_accumulate_like_numpy(self, dtype):
        rng = np.random.default_rng(5095)
        hist = Histogram2d(IRRELEVANT_TOPIC, (31, 17), (100, 999), (10, 90))
        expected = np.zeros(hist.shape)
        for pulse_time in range(3):
            tofs = rng.uniform(0, 1100, 2000).astype(dtype)
            dets = rng.uniform(0, 100, 2000).astype(dtype)

            hist.add_data(pulse_time, tofs, dets)

            expected += np.histogram2d(
                tofs, dets, bins=(31, 17), range=((100, 999), (10, 90))
            )[0]
        assert np.array_equal(hist.data, expected)

    @pytest.mark.parametrize("dtype", [np.int32, np.uint32])
    @pytest.mark.parametrize(
        "bins, tof_range, det_range, tof, det",
        [
            ((4, 4096), (0, 100), (0, 4096**2), 100, 4096**2 - 1),
            ((4096, 4), (0, 100_000_000), (0, 10), 99_999_999, 10),
            ((4, 4), (0, 10), (5, 5), 5, 5),
        ],
    )
    def test_2d_upper_edges_do_not_overflow_event_dtypes(
        self, dtype, bins, tof_range, det_range, tof, det
    ):
        tofs = np.array([tof, tof, tof_range[-1]], dtype=dtype)
        dets = np.array([det, det, det_range[-1]], dtype=dtype)
        hist = Histogram2d(IRRELEVANT_TOPIC, bins, tof_range, det_range)

        hist.add_data(1, tofs, dets)

        expected = np.histogram2d(tofs, dets, bins=bins, range=(tof_range, det_range))[
            0
        ]
        assert np.array_equal(hist.data, expected)
        assert hist.counts_sum() == 3

    @pytest.mark.parametrize("bins", [(7, 9), ([0.1, 0.3, 1.3], [10, 11, 11, 13])])
    def test_2d_boundary_pairs_match_numpy(self, bins):
        hist = Histogram2d(IRRELEVANT_TOPIC, bins, (0.1, 1.3), (10, 13))
        tofs = np.concatenate(
            (
                hist.x_edges,
                np.nextafter(hist.x_edges, -np.inf),
                np.nextafter(hist.x_edges, np.inf),
                [np.nan, np.inf],
            )
        )
        dets = np.concatenate(
            (
                hist.y_edges,
                np.nextafter(hist.y_edges, -np.inf),
                np.nextafter(hist.y_edges, np.inf),
                [np.nan, -np.inf],
            )
        )
        tofs, dets = (values.ravel() for values in np.meshgrid(tofs, dets))

        hist.add_data(1, tofs, dets)

        assert np.array_equal(
            hist.data,
            np.histogram2d(tofs, dets, bins=bins, range=((0.1, 1.3), (10, 13)))[0],
        )
