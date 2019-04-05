import numpy as np
from fast_histogram import histogram1d


class Histogrammer1d:
    """Histograms time-of-flight for a range of detectors into a 1-D histogram."""

    def __init__(self, topic, num_bins=50, tof_range=None, preprocessor=None, roi=None):
        """
        Constructor.

        Note on preprocessing functions: this should used for relatively low impact
        processing, i.e. avoid CPU intense algorithms.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param tof_range: The time-of-flight range to histogram over.
        :param preprocessor: The function to apply to the data before adding.
        :param roi: The function for checking data is within the region of interest.
        """
        self.histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.num_bins = num_bins
        self.topic = topic
        self.preprocessor = preprocessor
        self.roi = roi

    def add_data(self, pulse_time, x, y=None):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time.
        :param x: The time-of-flight data.
        :param y: Ignored parameter.
        """
        if self.preprocessor is not None:
            pulse_time, x, y = self._preprocess_data(pulse_time, x, y)

        if self.histogram is None:
            # If no tof range defined then generate one
            if self.tof_range is None:
                self.tof_range = (0, max(x))

            # Assumes that fast_histogram produces the same bins as numpy.
            self.x_edges = np.histogram_bin_edges(x, self.num_bins, self.tof_range)
            self.histogram = histogram1d(x, range=self.tof_range, bins=self.num_bins)
        else:
            self.histogram += histogram1d(x, range=self.tof_range, bins=self.num_bins)

    def _preprocess_data(self, pulse_time, x, y=None):
        """
        Apply the defined processing function to the data.

        :param pulse_time: The pulse time.
        :param x: The time-of-flight data.
        :param y: Ignored parameter.
        :return: The newly processed data
        """
        try:
            x = self.preprocessor(pulse_time, x)
        except Exception:
            # TODO: log
            print("Exception while preprocessing data")
        return pulse_time, x, y
