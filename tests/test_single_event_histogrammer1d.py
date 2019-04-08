import pytest
import math
from histograms.single_event_histogrammer1d import SingleEventHistogrammer1d


class TestSingleEventHistogrammer1d:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.num_bins = 5
        self.range = (0.0, 1 / 14 * 10 ** 9)
        self.hist = SingleEventHistogrammer1d("topic1", self.num_bins, self.range)

    def test_on_construction_histogram_is_uninitialised(self):
        assert self.hist.histogram is None
        assert self.hist.x_edges is None

    def test_adding_data_to_uninitialised_histogram_initialises_it(self):
        self.hist.add_data(1000)
        assert self.hist.histogram is not None
        assert self.hist.histogram.shape == (self.num_bins,)
        # Edges is 1 more than the number of bins
        assert len(self.hist.x_edges) == self.num_bins + 1
        assert self.hist.x_edges[0] == self.range[0]
        assert self.hist.x_edges[-1] == self.range[-1]

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

        assert self.hist.histogram[0] == 2
        assert self.hist.histogram[1] == 2
        assert self.hist.histogram[2] == 6
        assert self.hist.histogram[3] == 1
        assert self.hist.histogram[4] == 1

    def test_if_roi_function_supplied_then_outside_data_ignored(self):
        # Ignore outside ROI
        def _roi_check(event_time, x, detector):
            if detector in [3, 4]:
                return True
            return False

        hist = SingleEventHistogrammer1d(
            "topic1", self.num_bins, self.range, roi=_roi_check
        )

        event_times = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09]
        det_ids = [1, 2, 3, 4, 5, 6, 3, 4, 2]

        for et, d in zip(event_times, det_ids):
            # Must be in nanoseconds
            hist.add_data(et * 10 ** 9, detector=d)

        assert sum(hist.histogram) == 4
