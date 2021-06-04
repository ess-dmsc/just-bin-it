import numpy as np
import pytest

from just_bin_it.histograms.histogram2d_roi import RoiHistogram
from tests.test_histogram2d_map import generate_image

IRRELEVANT_TOPIC = "some-topic"
TOF_IS_IGNORED = None
EXPECTED_RESULT_AFTER_TWO_ADDS = [
    [32, 50, 68],
    [34, 52, 70],
    [36, 54, 72],
    [38, 56, 74],
]


class TestHistogramRoiFunctionality:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # A rhomboid 10 x 5 detector just to make things hard ;)
        self.width = 10
        self.height = 5
        self.roi_left_edges = [16, 25, 34]
        self.roi_width = 4
        self.shape = (self.roi_width, len(self.roi_left_edges))

        self.pulse_time = 1234
        self.data = generate_image(self.width, self.height)

        self.hist = RoiHistogram("topic", self.roi_left_edges, self.roi_width)

    def test_top_left_is_first_bin(self):
        assert self.hist.bins[0] == 16

    def test_last_bin_is_bottom_right_plus_two(self):
        assert self.hist.bins[~0] == 37 + 2

    def test_extra_ignored_bin_at_end_of_first_row(self):
        assert self.hist.bins[4] == 20

    def test_all_bins_are_correct(self):
        hand_calculated_bins = [
            16,
            17,
            18,
            19,
            20,
            25,
            26,
            27,
            28,
            29,
            34,
            35,
            36,
            37,
            38,
            39,
        ]
        assert np.array_equal(self.hist.bins, hand_calculated_bins)

    def test_adding_data_outside_bins_is_ignored(self):
        ids_outside = [15, 40]
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, ids_outside)

        assert self.hist.data.sum() == 0

    def test_shape_is_correct(self):
        assert self.hist.shape == self.shape

    def test_initial_outputted_data_is_correct_shape_and_all_zeros(self):
        assert self.hist.data.shape == self.shape
        assert self.hist.x_edges == [0, 1, 2, 3]
        assert self.hist.y_edges == [0, 1, 2]
        assert np.array_equal(self.hist.data, np.zeros(self.shape))

    def test_added_data_is_histogrammed_correctly(self):
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(self.hist.data, EXPECTED_RESULT_AFTER_TWO_ADDS)

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.identifier == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = RoiHistogram(
            IRRELEVANT_TOPIC, self.roi_left_edges, self.roi_width, identifier=example_id
        )
        assert hist.identifier == example_id

    def test_only_data_with_correct_source_is_added(self):
        hist = RoiHistogram(
            IRRELEVANT_TOPIC, self.roi_left_edges, self.roi_width, source="source1"
        )

        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data, source="source1")
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data, source="source1")
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data, source="OTHER")

        assert np.array_equal(hist.data, EXPECTED_RESULT_AFTER_TWO_ADDS)

    def test_clearing_histogram_data_clears_histogram(self):
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        self.hist.clear_data()

        assert self.hist.data.sum() == 0

    def test_after_clearing_histogram_can_add_data(self):
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)
        self.hist.clear_data()

        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(self.hist.data, EXPECTED_RESULT_AFTER_TWO_ADDS)

    def test_adding_empty_data_does_nothing(self):
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, [])

        assert self.hist.data.sum() == 0

    def test_histogram_keeps_track_of_last_pulse_time_processed(self):
        self.hist.add_data(1234, TOF_IS_IGNORED, self.data)
        self.hist.add_data(1235, TOF_IS_IGNORED, self.data)
        self.hist.add_data(1236, TOF_IS_IGNORED, self.data)

        assert self.hist.last_pulse_time == 1236


class TestHistogramRoiEdgeCases:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # Use a rectangular detector of 5 x 4
        self.data = generate_image(5, 4)
        self.pulse_time = 1234

    def test_when_roi_is_the_whole_detector_it_still_works(self):
        left_edges = [1, 6, 11, 16]
        roi_width = 5
        expected_result = [
            [1, 6, 11, 16],
            [2, 7, 12, 17],
            [3, 8, 13, 18],
            [4, 9, 14, 19],
            [5, 10, 15, 20],
        ]
        hist = RoiHistogram("topic", left_edges, roi_width)

        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)

    def test_roi_with_one_pixel_border(self):
        left_edges = [7, 12]
        roi_width = 3
        expected_result = [[7, 12], [8, 13], [9, 14]]

        hist = RoiHistogram("topic", left_edges, roi_width)
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)

    def test_top_left_roi(self):
        left_edges = [1, 6]
        roi_width = 2
        expected_result = [[1, 6], [2, 7]]

        hist = RoiHistogram("topic", left_edges, roi_width)
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)

    def test_top_right_roi(self):
        left_edges = [4, 9]
        roi_width = 2
        expected_result = [[4, 9], [5, 10]]

        hist = RoiHistogram("topic", left_edges, roi_width)
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)

    def test_bottom_left_roi(self):
        left_edges = [11, 16]
        roi_width = 2
        expected_result = [[11, 16], [12, 17]]

        hist = RoiHistogram("topic", left_edges, roi_width)
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)

    def test_bottom_right_roi(self):
        left_edges = [14, 19]
        roi_width = 2
        expected_result = [[14, 19], [15, 20]]

        hist = RoiHistogram("topic", left_edges, roi_width)
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)

    @pytest.mark.parametrize(
        "left_edges, expected_result",
        [
            ([1], [[1]]),
            ([5], [[5]]),
            ([7], [[7]]),
            ([14], [[14]]),
            ([16], [[16]]),
            ([20], [[20]]),
        ],
    )
    def test_roi_of_single_pixel(self, left_edges, expected_result):
        roi_width = 1

        hist = RoiHistogram("topic", left_edges, roi_width)
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(hist.data, expected_result)
