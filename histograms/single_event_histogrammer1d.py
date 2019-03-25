import numpy as np
from fast_histogram import histogram1d
import math


class SingleEventHistogrammer1d:
    def __init__(self, topic, num_bins=50, tof_range=None, preprocessor=None):
        """
        Constructor.

        Note on preprocessing functions: this should used for relatively low impact
        processing, i.e. avoid CPU intense algorithms.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param tof_range: The time-of-flight range to histogram over (nanoseconds).
        :param preprocessor: The function to apply to the data before adding.
        """
        self.histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.num_bins = num_bins
        self.topic = topic
        self.preprocessor = preprocessor

        # Create a list of pulse times assuming the first pulse is at 0.00
        # i.e on the second.
        pulse_freq = 14
        self.pulse_times = []
        for i in range(pulse_freq):
            # In nanoseconds
            self.pulse_times.append(math.floor(i / pulse_freq * 10 ** 9))
        self.pulse_times.append(10 ** 9)

    def add_data(self, event_time, x=None, y=None):
        """
        Add data to the histogram.

        :param event_time: The pulse time which is used as the event time.
        :param x: Ignored parameter.
        :param y: Ignored parameter.
        """
        # Throw away the seconds part
        nanosecs = event_time % 1_000_000_000

        bin_num = np.digitize([nanosecs], self.pulse_times)

        corrected_time = nanosecs - self.pulse_times[bin_num[0] - 1]

        if self.histogram is None:
            # Assumes that fast_histogram produces the same bins as numpy.
            self.x_edges = np.histogram_bin_edges(
                [corrected_time], self.num_bins, self.tof_range
            )
            self.histogram = histogram1d(
                [corrected_time], range=self.tof_range, bins=self.num_bins
            )
        else:
            self.histogram += histogram1d(
                [corrected_time], range=self.tof_range, bins=self.num_bins
            )
