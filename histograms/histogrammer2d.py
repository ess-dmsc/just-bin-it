import numpy as np


class Histogrammer2d:
    def __init__(self, tof_range, det_range, num_bins, topic):
        """
        Constructor.

        :param tof_range: The range of time-of-flights to histogram over.
        :param det_range: The range of sequential detectors to histogram over.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param topic: The name of the Kafka topic to publish to.
        """
        self.histogram = None
        self.x_edges = None
        self.y_edges = None
        self.tof_range = tof_range
        self.det_range = det_range
        self.num_bins = num_bins
        self.topic = topic

    def add_data(self, x, y):
        """
        Add data to the histogram.

        :param x: The time-of-flight data.
        :param y: The detector data.
        """
        if self.histogram is None:
            self.histogram, self.x_edges, self.y_edges = np.histogram2d(
                x, y, range=(self.tof_range, self.det_range), bins=self.num_bins
            )
        else:
            self.histogram += np.histogram2d(
                x,
                y,
                range=(self.tof_range, self.det_range),
                bins=(self.x_edges, self.y_edges),
            )[0]
