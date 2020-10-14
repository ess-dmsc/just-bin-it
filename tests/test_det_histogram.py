import numpy as np
import pytest

from just_bin_it.exceptions import JustBinItException
from just_bin_it.histograms.det_histogram import DetHistogram

IRRELEVANT_TOPIC = "some-topic"
IRRELEVANT_TOF_RANGE = (0, 100)
IRRELEVANT_DET_RANGE = (0, 100)
IRRELEVANT_WIDTH = 100
IRRELEVANT_HEIGHT = 100


def generate_pixel_id(x, y, width):
    return y * width + x + 1


class TestHistogram2dMapFunctionality:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.tof_range = (0, 10)
        self.det_range = (1, 25)
        self.width = 5
        self.height = 5
        self.data = []
        for x in range(self.width):
            for y in range(self.height):
                self.data.append(generate_pixel_id(x, y, self.width))
        self.hist = DetHistogram(
            "topic", self.tof_range, self.det_range, self.width, self.height
        )

    def test_adding_data_to_offset_detector_range_is_okay(self):
        hist = DetHistogram(
            IRRELEVANT_TOPIC, self.tof_range, (3, 27), self.width, self.height
        )

        hist.add_data(self.pulse_time, [], self.data)

        assert hist.data.sum() == 23

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.x_edges is not None
        assert self.hist.shape == (5, 5)
        assert len(self.hist.x_edges) == 6
        assert len(self.hist.y_edges) == 6
        assert self.hist.x_edges[0] == 0
        assert self.hist.x_edges[-1] == 5
        assert self.hist.y_edges[0] == 0
        assert self.hist.y_edges[-1] == 5
        assert self.hist.data.sum() == 0

    def test_adding_data_to_initialised_histogram_new_data_is_added(self):
        self.hist.add_data(self.pulse_time, [], self.data)
        first_sum = self.hist.data.sum()

        # Add the data again
        self.hist.add_data(self.pulse_time, [], self.data)

        # Sum should be double
        assert self.hist.data.sum() == first_sum * 2

    def test_adding_data_outside_bins_is_ignored(self):
        self.hist.add_data(self.pulse_time, [], self.data)
        first_sum = self.hist.data.sum()
        x_edges = self.hist.x_edges[:]

        # Add data that is outside the edges
        new_data = np.array([0, 26, 27, 100])
        self.hist.add_data(self.pulse_time, [], new_data)

        # Sum should not change
        assert self.hist.data.sum() == first_sum
        # Edges should not change
        assert np.array_equal(self.hist.x_edges, x_edges)

    def test_adding_data_of_specific_shape_is_captured(self):
        p2_2 = generate_pixel_id(2, 2, self.width)
        p3_2 = generate_pixel_id(3, 2, self.width)
        p0_3 = generate_pixel_id(0, 3, self.width)

        data = [p2_2, p2_2, p3_2, p3_2, p3_2, p0_3, p0_3]
        self.hist.add_data(self.pulse_time, [], data)

        assert self.hist.data.sum() == len(data)
        assert self.hist.data[2][2] == 2
        assert self.hist.data[3][2] == 3
        assert self.hist.data[0][3] == 2

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.identifier == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = DetHistogram(
            IRRELEVANT_TOPIC,
            self.tof_range,
            self.det_range,
            self.width,
            self.height,
            identifier=example_id,
        )
        assert hist.identifier == example_id

    def test_only_data_with_correct_source_is_added(self):
        hist = DetHistogram(
            IRRELEVANT_TOPIC,
            self.tof_range,
            self.det_range,
            self.width,
            self.height,
            source="source1",
        )

        hist.add_data(self.pulse_time, [], self.data, source="source1")
        hist.add_data(self.pulse_time, [], self.data, source="source1")
        hist.add_data(self.pulse_time, [], self.data, source="OTHER")

        assert hist.data.sum() == len(self.data) * 2

    def test_clearing_histogram_data_clears_histogram(self):
        self.hist.add_data(self.pulse_time, [], self.data)

        self.hist.clear_data()

        assert self.hist.data.sum() == 0

    def test_after_clearing_histogram_can_add_data(self):
        self.hist.add_data(self.pulse_time, [], self.data)
        self.hist.clear_data()

        self.hist.add_data(self.pulse_time, [], self.data)

        assert self.hist.shape == (5, 5)
        assert self.hist.data.sum() == len(self.data)

    def test_adding_empty_data_does_nothing(self):
        self.hist.add_data(self.pulse_time, [], [])

        assert self.hist.data.sum() == 0

    def test_histogram_keeps_track_of_last_pulse_time_processed(self):
        self.hist.add_data(1234, [], self.data)
        self.hist.add_data(1235, [], self.data)
        self.hist.add_data(1236, [], self.data)

        assert self.hist.last_pulse_time == 1236


class TestHistogram2dMapConstruction:
    def test_if_tof_missing_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                None,
                IRRELEVANT_DET_RANGE,
                IRRELEVANT_WIDTH,
                IRRELEVANT_HEIGHT,
            )

    def test_if_tof_is_not_two_values_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                (1,),
                IRRELEVANT_DET_RANGE,
                IRRELEVANT_WIDTH,
                IRRELEVANT_HEIGHT,
            )

    def test_if_width_not_numeric_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                IRRELEVANT_TOF_RANGE,
                IRRELEVANT_DET_RANGE,
                None,
                IRRELEVANT_HEIGHT,
            )

    def test_if_width_not_greater_than_0_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                IRRELEVANT_TOF_RANGE,
                IRRELEVANT_DET_RANGE,
                0,
                IRRELEVANT_HEIGHT,
            )

    def test_if_height_not_numeric_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                IRRELEVANT_TOF_RANGE,
                IRRELEVANT_DET_RANGE,
                IRRELEVANT_WIDTH,
                None,
            )

    def test_if_height_not_greater_than_0_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                IRRELEVANT_TOF_RANGE,
                IRRELEVANT_DET_RANGE,
                IRRELEVANT_WIDTH,
                0,
            )

    def test_if_det_range_is_missing_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                IRRELEVANT_TOF_RANGE,
                None,
                IRRELEVANT_WIDTH,
                IRRELEVANT_HEIGHT,
            )

    def test_if_det_range_is_not_two_values_then_histogram_not_created(self):
        with pytest.raises(JustBinItException):
            DetHistogram(
                IRRELEVANT_TOPIC,
                IRRELEVANT_TOF_RANGE,
                (1,),
                IRRELEVANT_WIDTH,
                IRRELEVANT_HEIGHT,
            )

    def test_if_no_id_specified_then_empty_string(self):
        histogram = DetHistogram(
            IRRELEVANT_TOPIC,
            IRRELEVANT_TOF_RANGE,
            IRRELEVANT_DET_RANGE,
            IRRELEVANT_WIDTH,
            IRRELEVANT_HEIGHT,
        )

        assert histogram.identifier == ""

    def test_config_with_id_specified_sets_id(self):
        histogram = DetHistogram(
            IRRELEVANT_TOPIC,
            IRRELEVANT_TOF_RANGE,
            IRRELEVANT_DET_RANGE,
            IRRELEVANT_WIDTH,
            IRRELEVANT_HEIGHT,
            identifier="123456",
        )

        assert histogram.identifier == "123456"
