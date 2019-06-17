import pytest
import math
from histograms.single_event_histogram1d import SingleEventHistogram1d


class TestSingleEventHistogram1d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.num_bins = 5
        self.range = (0.0, 1 / 14 * 10 ** 9)
        self.hist = SingleEventHistogram1d("topic1", self.num_bins, self.range)

    def test_on_construction_histogram_is_initialised_empty(self):
        assert self.hist.x_edges is not None
        assert self.hist.shape == (self.num_bins,)
        assert len(self.hist.x_edges) == self.num_bins + 1
        assert self.hist.x_edges[0] == self.range[0]
        assert self.hist.x_edges[-1] == self.range[-1]

    def test_adding_data_to_histogram_adds_data(self):
        self.hist.add_data(0.01)
        self.hist.add_data(0.02)
        self.hist.add_data(0.03)

        assert sum(self.hist.data) == 3

    def test_pulse_times_are_correctly_initialised_in_nanoseconds(self):
        assert len(self.hist.pulse_times) == 15
        assert self.hist.pulse_times[0] == 0
        assert self.hist.pulse_times[1] == math.floor(1 / 14 * 10 ** 9)
        assert self.hist.pulse_times[2] == math.floor(2 / 14 * 10 ** 9)
        assert self.hist.pulse_times[6] == math.floor(6 / 14 * 10 ** 9)
        assert self.hist.pulse_times[10] == math.floor(10 / 14 * 10 ** 9)
        assert self.hist.pulse_times[13] == math.floor(13 / 14 * 10 ** 9)

    def test_event_times_are_corrected_wrt_which_pulse_they_are_in(self):
        # event in the "first" pulse, should be histogrammed as 1, 1, 2, 0, 0
        event_times = [0.01, 0.02, 0.03, 0.04]
        # event in the "second" pulse, should be histogrammed as 0, 0, 2, 1, 1
        event_times.extend([0.03 + 1 / 14, 0.04 + 1 / 14, 0.05 + 1 / 14, 0.06 + 1 / 14])
        # event in much later pulse, should be histogrammed as 1, 1, 2, 0, 0
        event_times.extend([123.01, 123.02, 123.03, 123.04])

        for et in event_times:
            # Must be in nanoseconds
            self.hist.add_data(et * 10 ** 9)

        assert self.hist.data[0] == 2
        assert self.hist.data[1] == 2
        assert self.hist.data[2] == 6
        assert self.hist.data[3] == 1
        assert self.hist.data[4] == 1

    def test_if_roi_function_supplied_then_outside_data_ignored(self):
        # Ignore outside ROI
        def _create_mask(event_time, x, detector):
            if detector in [3, 4]:
                return [0]
            else:
                return [1]

        hist = SingleEventHistogram1d(
            "topic1", self.num_bins, self.range, roi=_create_mask
        )

        event_times = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09]
        det_ids = [1, 2, 3, 4, 5, 6, 3, 4, 2]

        for et, d in zip(event_times, det_ids):
            # Must be in nanoseconds
            hist.add_data(et * 10 ** 9, det_ids=d)

        assert sum(hist.data) == 4

    def test_only_data_with_correct_source_is_added(self):
        hist = SingleEventHistogram1d(
            "topic1", self.num_bins, self.range, source="source1"
        )

        hist.add_data(0.01, 1, source="source1")
        hist.add_data(0.02, 2, source="source1")
        hist.add_data(0.03, 3, source="OTHER")

        assert sum(hist.data) == 2

    def test_if_no_id_supplied_then_defaults_to_empty_string(self):
        assert self.hist.id == ""

    def test_id_supplied_then_is_set(self):
        example_id = "abcdef"
        hist = SingleEventHistogram1d(
            "topic1", self.num_bins, self.range, id=example_id
        )
        assert hist.id == example_id
