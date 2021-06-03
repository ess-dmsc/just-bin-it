import numpy as np
import pytest

from just_bin_it.histograms.histogram2d_roi import RoiHistogram
from tests.test_histogram2d_map import generate_image

IRRELEVANT_TOPIC = "some-topic"
TOF_IS_IGNORED = None
IRRELEVANT_DET_RANGE = (0, 100)
IRRELEVANT_WIDTH = 100
IRRELEVANT_HEIGHT = 100


class TestHistogramRoiFunctionality:
    @pytest.fixture(autouse=True)
    def prepare(self):
        # A rhomboid 10 x 5 detector just to make things hard ;)
        self.width = 10
        self.height = 5
        self.roi_left_edges = [16, 25, 34]
        self.roi_width = 4

        self.pulse_time = 1234
        self.data = generate_image(self.width, self.height)

        self.hist = RoiHistogram("topic", self.roi_left_edges, self.roi_width)

    def test_top_left_is_first_bin(self):
        assert self.hist.x_edges[0] == 16

    def test_last_bin_is_bottom_right_plus_two(self):
        assert self.hist.x_edges[~0] == 37 + 2

    def test_extra_ignored_bin_at_end_of_first_row(self):
        assert self.hist.x_edges[4] == 20

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
        assert np.array_equal(self.hist.x_edges, hand_calculated_bins)

    def test_adding_data_outside_bins_is_ignored(self):
        ids_outside = [15, 40]
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, ids_outside)

        assert self.hist.data.sum() == 0

    def test_shape_is_correct(self):
        assert self.hist.shape == (3, 4)

    def test_initial_outputted_data_is_correct_shape_and_all_zeros(self):
        data = self.hist.data
        assert data.shape == (3, 4)
        assert np.array_equal(data, np.zeros((3, 4)))

    def test_added_data_is_histogrammed_correctly(self):
        expected_result = [[32, 34, 36, 38], [50, 52, 54, 56], [68, 70, 72, 74]]

        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)
        self.hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data)

        assert np.array_equal(self.hist.data, expected_result)

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.identifier == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = RoiHistogram(
            IRRELEVANT_TOPIC, self.roi_left_edges, self.roi_width, identifier=example_id
        )
        assert hist.identifier == example_id

    def test_only_data_with_correct_source_is_added(self):
        expected_result = [[32, 34, 36, 38], [50, 52, 54, 56], [68, 70, 72, 74]]

        hist = RoiHistogram(
            IRRELEVANT_TOPIC, self.roi_left_edges, self.roi_width, source="source1"
        )

        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data, source="source1")
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data, source="source1")
        hist.add_data(self.pulse_time, TOF_IS_IGNORED, self.data, source="OTHER")

        assert np.array_equal(hist.data, expected_result)


#
#     def test_clearing_histogram_data_clears_histogram(self):
#         self.hist.add_data(self.pulse_time, [], self.data)
#
#         self.hist.clear_data()
#
#         assert self.hist.data.sum() == 0
#
#     def test_after_clearing_histogram_can_add_data(self):
#         self.hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data)
#         self.hist.clear_data()
#
#         self.hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data)
#
#         assert self.hist.shape == (5, 5)
#         assert self.hist.data.sum() == len(self.data)
#
#     def test_adding_empty_data_does_nothing(self):
#         self.hist.add_data(self.pulse_time, [], [])
#
#         assert self.hist.data.sum() == 0
#
#     def test_histogram_keeps_track_of_last_pulse_time_processed(self):
#         self.hist.add_data(1234, [], self.data)
#         self.hist.add_data(1235, [], self.data)
#         self.hist.add_data(1236, [], self.data)
#
#         assert self.hist.last_pulse_time == 1236
#
#
# class TestHistogram2dMapConstruction:
#     def test_if_no_id_specified_then_empty_string(self):
#         histogram = DetHistogram(
#             IRRELEVANT_TOPIC, IRRELEVANT_DET_RANGE, IRRELEVANT_WIDTH, IRRELEVANT_HEIGHT
#         )
#
#         assert histogram.identifier == ""
#
#     def test_config_with_id_specified_sets_id(self):
#         histogram = DetHistogram(
#             IRRELEVANT_TOPIC,
#             IRRELEVANT_DET_RANGE,
#             IRRELEVANT_WIDTH,
#             IRRELEVANT_HEIGHT,
#             identifier="123456",
#         )
#
#         assert histogram.identifier == "123456"
