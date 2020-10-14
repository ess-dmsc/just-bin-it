import logging

import numpy as np
from fast_histogram import histogram1d

from just_bin_it.histograms.input_validators import (
    check_bins,
    check_det_range,
    check_tof,
    generate_exception,
)


def _validate_parameters(num_bins, tof_range, det_range):
    """
    Checks that the parameters are defined correctly, if not throw.

    Note: probably not entirely bullet-proof but a good first defence.

    :param num_bins: The number of histogram bins.
    :param tof_range: The time-of-flight range.
    :param det_range: The detector range (optional).
    """
    missing = []
    invalid = []

    check_tof(tof_range, missing, invalid)
    check_bins(num_bins, missing, invalid)

    # det_range is optional
    if det_range:
        check_det_range(det_range, missing, invalid)

    if missing or invalid:
        generate_exception(missing, invalid, "1D")


class Histogram1d:
    """One dimensional histogram for time-of-flight."""

    def __init__(
        self, topic, num_bins, tof_range, det_range=None, source="", identifier=""
    ):
        """
        Constructor.

        Note on preprocessing functions: this should used for relatively low impact
        processing, i.e. avoid CPU intense algorithms.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param tof_range: The time-of-flight range to histogram over.
        :param det_range: The detector range to include data from.
        :param source: The data source to histogram.
        :param identifier: An optional identifier for the histogram.
        """
        _validate_parameters(num_bins, tof_range, det_range)

        self._histogram = None
        self.x_edges = None
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
        self.x_edges = np.histogram_bin_edges([], self.num_bins, self.tof_range)
        self._histogram = histogram1d([], range=self.tof_range, bins=self.num_bins)

    def add_data(self, pulse_time, tofs, det_ids=None, source=""):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time.
        :param tofs: The time-of-flight data.
        :param det_ids: The detector ids.
        :param source: The source of the event.
        """
        # Discard any messages not from the specified source.
        if self.source is not None and source != self.source:
            return

        self.last_pulse_time = pulse_time

        if self.det_range:
            # Create 2D histogram so we can filter on det-id then reduce to 1D.
            # This is the quickest way to filter on det-id.
            histogram, _, _ = np.histogram2d(
                tofs,
                det_ids,
                range=(self.tof_range, self.det_range),
                bins=self.num_bins,
            )
            self._histogram += histogram.sum(1)
        else:
            self._histogram += histogram1d(
                tofs, range=self.tof_range, bins=self.num_bins
            )

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
