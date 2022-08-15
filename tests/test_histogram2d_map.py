import numpy as np
import pytest

from just_bin_it.histograms.histogram2d_map import DetHistogram

IRRELEVANT_TOPIC = "some-topic"
IRRELEVANT_TOF_RANGE = (0, 100)


def generate_pixel_id(x, y, width):
    return y * width + x + 1


def generate_image(width, height):
    # First pixel gets one hit, second gets two, third get three and so on.
    det_ids = []
    count = 1
    for h in range(height):
        for w in range(width):
            det_ids.extend([count] * count)
            count += 1
    return det_ids


class TestHistogram2dMapFunctionality:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.pulse_time = 1234
        self.det_range = (1, 31)
        self.width = 5
        self.height = 6
        self.data = generate_image(self.width, self.height)

        self.hist = DetHistogram("topic", self.det_range, self.width, self.height)

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.x_edges is not None
        assert self.hist.shape == (self.width, self.height)
        assert len(self.hist.x_edges) == self.width + 1
        assert len(self.hist.y_edges) == self.height + 1
        assert self.hist.x_edges[0] == 0
        assert self.hist.x_edges[-1] == 5
        assert self.hist.y_edges[0] == 0
        assert self.hist.y_edges[-1] == 6
        assert self.hist.data.sum() == 0

    def test_data_before_detector_range_ignored(self):
        hist = DetHistogram(IRRELEVANT_TOPIC, (3, 32), self.width, self.height)

        hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data)

        # The data for det_ids 1 and 2 are discarded.
        assert hist.data.sum() == len(self.data) - self.data.count(1) - self.data.count(
            2
        )
        assert hist.data[3][5] == 0
        assert hist.data[4][5] == 0

    def test_data_after_detector_range_ignored(self):
        hist = DetHistogram(IRRELEVANT_TOPIC, (0, 30), self.width, self.height)

        hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data)

        # data for det id 30 should not be counted
        assert hist.data.sum() == len(self.data) - self.data.count(30)

    def test_check_binning(self):
        self.hist.add_data(self.pulse_time, [], self.data)

        shape = self.hist.shape
        data = self.hist.data

        # The data should be 1 for the first bin, 2 for the second, and so on...
        expected = 0
        for y in range(shape[1]):
            for x in range(shape[0]):
                expected += 1
                assert data[x][y] == expected

    def test_adding_data_accumulates_histogram(self):
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
        new_data = np.array([0, 31, 32, 100])
        self.hist.add_data(self.pulse_time, [], new_data)

        # Sum should not change
        assert self.hist.data.sum() == first_sum
        # Edges should not change
        assert np.array_equal(self.hist.x_edges, x_edges)

    def test_adding_data_of_specific_shape_is_captured(self):
        p0_0 = generate_pixel_id(0, 0, self.width)
        p3_2 = generate_pixel_id(3, 2, self.width)
        p0_3 = generate_pixel_id(0, 3, self.width)
        p4_5 = generate_pixel_id(4, 5, self.width)

        data = [p0_0, p0_0, p3_2, p3_2, p3_2, p0_3, p0_3, p4_5]
        self.hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, data)

        assert self.hist.data.sum() == len(data)
        assert self.hist.data[0][0] == 2
        assert self.hist.data[3][2] == 3
        assert self.hist.data[0][3] == 2
        assert self.hist.data[4][5] == 1

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.identifier == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = DetHistogram(
            IRRELEVANT_TOPIC,
            self.det_range,
            self.width,
            self.height,
            identifier=example_id,
        )
        assert hist.identifier == example_id

    def test_only_data_with_correct_source_is_added(self):
        hist = DetHistogram(
            IRRELEVANT_TOPIC, self.det_range, self.width, self.height, source="source1"
        )

        hist.add_data(
            self.pulse_time, IRRELEVANT_TOF_RANGE, self.data, source="source1"
        )
        hist.add_data(
            self.pulse_time, IRRELEVANT_TOF_RANGE, self.data, source="source1"
        )
        hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data, source="OTHER")

        assert hist.data.sum() == len(self.data) * 2

    def test_clearing_histogram_data_clears_histogram(self):
        self.hist.add_data(self.pulse_time, [], self.data)

        self.hist.clear_data()

        assert self.hist.data.sum() == 0

    def test_after_clearing_histogram_can_add_data(self):
        self.hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data)
        self.hist.clear_data()

        self.hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, self.data)

        assert self.hist.shape == (self.width, self.height)
        assert self.hist.data.sum() == len(self.data)

    def test_adding_empty_data_does_nothing(self):
        self.hist.add_data(self.pulse_time, IRRELEVANT_TOF_RANGE, [])

        assert self.hist.data.sum() == 0

    def test_histogram_keeps_track_of_last_pulse_time_processed(self):
        self.hist.add_data(1234, IRRELEVANT_TOF_RANGE, self.data)
        self.hist.add_data(1235, IRRELEVANT_TOF_RANGE, self.data)
        self.hist.add_data(1236, IRRELEVANT_TOF_RANGE, self.data)

        assert self.hist.last_pulse_time == 1236

    def test_histograms_corners(self):
        # Test reproduces a bug that was fixed
        self.hist.add_data(1234, IRRELEVANT_TOF_RANGE, [1])
        self.hist.add_data(1235, IRRELEVANT_TOF_RANGE, [5, 5])
        self.hist.add_data(1236, IRRELEVANT_TOF_RANGE, [26, 26, 26])
        self.hist.add_data(1236, IRRELEVANT_TOF_RANGE, [30, 30, 30, 30])

        assert self.hist.data[0][0] == 1
        assert self.hist.data[4][0] == 2
        assert self.hist.data[0][5] == 3
        assert self.hist.data[4][5] == 4
