import numpy as np
from fast_histogram import histogram1d
import logging


class DetHistogram:
    """Two dimensional histogram for detectors."""

    def __init__(self, topic, tof_range, det_range, width, source=None, identifier=""):
        """
        Constructor.

        :param topic: The name of the Kafka topic to publish to.
        :param tof_range: The range of time-of-flights to histogram over.
        :param source: The data source to histogram.
        :param det_range: The range of sequential detectors to histogram over.
        :param width: How many detectors in a row.
        :param identifier: An optional identifier for the histogram.
        """
        self._histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.det_range = det_range
        # The number of bins is the number of detectors.
        self.num_bins = det_range[1] - det_range[0] + 1
        self.width = width
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source

        self._intialise_histogram()

    def _intialise_histogram(self):
        """
        Create a zeroed histogram with the correct shape.
        """
        # If the number of detectors is not a multiple of the width, then cannot make a
        # rectangle
        if self.num_bins % self.width != 0:
            raise Exception("Detector range and width are incompatible")

        self.x_edges = np.histogram_bin_edges([], self.num_bins, self.det_range)
        self._histogram = histogram1d([], range=self.det_range, bins=self.num_bins)

    @property
    def data(self):
        return self._histogram

    @property
    def shape(self):
        return self._histogram.shape

    def add_data(self, pulse_time, tof, det_ids, source=""):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time.
        :param tof: The time-of-flight data.
        :param det_ids: The detector data.
        :param source: The source of the event.
        """
        # Discard any messages not from the specified source.
        if self.source is not None and source != self.source:
            return

        self.last_pulse_time = pulse_time

        self._histogram += histogram1d(
            det_ids, range=self.det_range, bins=self.num_bins
        )

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._intialise_histogram()
