import numpy as np


class Histogram2d:
    def __init__(self, topic, num_bins, tof_range, det_range):
        """
        Constructor.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the data up into.
        :param tof_range: The range of time-of-flights to histogram over.
        :param det_range: The range of sequential detectors to histogram over.
        """
        self._histogram = None
        self.x_edges = None
        self.y_edges = None
        self.tof_range = tof_range
        self.det_range = det_range
        self.num_bins = num_bins
        self.topic = topic
        self.last_pulse_time = 0

    def add_data(self, pulse_time, x, y):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time.
        :param x: The time-of-flight data.
        :param y: The detector data.
        """
        if self._histogram is None:
            self._histogram, self.x_edges, self.y_edges = np.histogram2d(
                x, y, range=(self.tof_range, self.det_range), bins=self.num_bins
            )
        else:
            self._histogram += np.histogram2d(
                x,
                y,
                range=(self.tof_range, self.det_range),
                bins=(self.x_edges, self.y_edges),
            )[0]

    @property
    def data(self):
        return self._histogram

    @property
    def shape(self):
        return self._histogram.shape
