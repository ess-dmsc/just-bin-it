import logging
import math
import numpy as np
from fast_histogram import histogram1d


class SingleEventHistogram1d:
    """Histograms time-of-flight for the denex detector into a 1-D histogram."""

    def __init__(
        self,
        topic,
        num_bins=50,
        tof_range=None,
        source=None,
        identifier="",
    ):
        """
        Constructor.

        Note on preprocessing functions: this should used for relatively low impact
        processing, i.e. avoid CPU intense algorithms.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param tof_range: The time-of-flight range to histogram over (nanoseconds).
        :param source: The data source to histogram.
        :param identifier: An optional identifier for the histogram.
        """
        self._histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.num_bins = num_bins
        self.topic = topic
        self.source = source
        self.identifier = identifier

        self.last_pulse_time = 0

        # Create a list of pulse times assuming the first pulse is at 0.00
        # i.e on the second.
        pulse_freq = 14
        self.pulse_times = []
        for i in range(pulse_freq):
            # In nanoseconds
            self.pulse_times.append(math.floor(i / pulse_freq * 10 ** 9))
        self.pulse_times.append(10 ** 9)

        self._intialise_histogram()

    def _intialise_histogram(self):
        """
        Create a zeroed histogram with the correct shape.
        """
        # Assumes that fast_histogram produces the same bins as numpy.
        self.x_edges = np.histogram_bin_edges([], self.num_bins, self.tof_range)
        self._histogram = histogram1d([], range=self.tof_range, bins=self.num_bins)

    def add_data(self, pulse_time, tofs=None, det_ids=None, source=""):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time which is used as the event time.
        :param tofs: Ignored parameter.
        :param det_ids: An array of one value which is the pixel hit.
        :param source: The source of the event.
        """
        # Discard any messages not from the specified source.
        if self.source is not None and source != self.source:
            return

        # Throw away the seconds part
        nanosecs = pulse_time % 1_000_000_000

        self.last_pulse_time = nanosecs

        bin_num = np.digitize([nanosecs], self.pulse_times)

        corrected_time = nanosecs - self.pulse_times[bin_num[0] - 1]

        self._histogram += histogram1d(
            [corrected_time], range=self.tof_range, bins=self.num_bins
        )

    @property
    def data(self):
        return self._histogram

    @property
    def shape(self):
        return self._histogram.shape
