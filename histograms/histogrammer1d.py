import numpy as np
from fast_histogram import histogram1d


class Histogrammer1d:
    def __init__(self, det_range, num_bins, topic):
        """
        Constructor.

        :param det_range: The range of sequential detectors to histogram over.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param topic: The name of the Kafka topic to publish to.
        """
        self.histogram = None
        self.x_edges = None
        self.det_range = det_range
        self.num_bins = num_bins
        self.topic = topic

    def add_data(self, x, y=None):
        """
        Add data to the histogram.

        :param x: The time-of-flight data.
        :param y: Ignored parameter.
        """
        if self.histogram is None:
            # Assumes that fast_histogram produces the same bins as numpy.
            self.x_edges = np.histogram_bin_edges(x, self.num_bins, self.det_range)
            self.histogram = histogram1d(x, range=self.det_range, bins=self.num_bins)
        else:
            self.histogram += histogram1d(x, range=self.det_range, bins=self.num_bins)
