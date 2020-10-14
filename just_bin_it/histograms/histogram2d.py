import logging

import numpy as np

from just_bin_it.histograms.input_validators import (
    check_bins,
    check_det_range,
    check_tof,
    generate_exception,
)


def _validate_parameters(num_bins, tof_range, det_range):
    """
    Checks that the required parameters are defined, if not throw.

    Note: probably not entirely bullet-proof but a good first defence.

    :param num_bins: The number of histogram bins.
    :param tof_range: The time-of-flight range.
    :param det_range: The detector range.
    """
    missing = []
    invalid = []

    check_tof(tof_range, missing, invalid)
    check_det_range(det_range, missing, invalid)
    check_bins(num_bins, missing, invalid)
    if missing or invalid:
        generate_exception(missing, invalid, "2D")


class Histogram2d:
    """Two dimensional histogram for time-of-flight."""

    def __init__(self, topic, num_bins, tof_range, det_range, source="", identifier=""):
        """
        Constructor.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the data up into.
        :param tof_range: The range of time-of-flights to histogram over.
        :param det_range: The range of sequential detectors to histogram over.
        :param source: The data source to histogram.
        :param identifier: An optional identifier for the histogram.
        """
        _validate_parameters(num_bins, tof_range, det_range)
        self._histogram = None
        self.x_edges = None
        self.y_edges = None
        self.tof_range = tof_range
        self.det_range = det_range
        self.num_bins = num_bins
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source if source.strip() != "" else None

        self._intialise_histogram()

    def _intialise_histogram(self):
        """
        Create a zeroed histogram with the correct shape.
        """
        self._histogram, self.x_edges, self.y_edges = np.histogram2d(
            [], [], range=(self.tof_range, self.det_range), bins=self.num_bins
        )

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

        self._histogram += np.histogram2d(
            tof, det_ids, range=(self.tof_range, self.det_range), bins=self.num_bins
        )[0]

    @property
    def data(self):
        return self._histogram

    @property
    def shape(self):
        return self._histogram.shape

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._intialise_histogram()
