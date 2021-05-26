import logging

import numpy as np

from just_bin_it.histograms.input_validators import (
    check_det_range,
    check_int,
    generate_exception,
)


def _validate_parameters(det_range, width, height):
    """
    Checks that the required parameters are defined, if not throw.

    Note: probably not entirely bullet-proof but a good first defence.

    :param det_range: The detector range.
    :param width: The detector width.
    :param height: The detector height.
    """
    missing = []
    invalid = []

    check_det_range(det_range, missing, invalid)
    check_int(width, "width", invalid)
    check_int(height, "height", invalid)
    if missing or invalid:
        generate_exception(missing, invalid, "2D Map")


class DetHistogram:
    """Two dimensional histogram for detectors."""

    def __init__(self, topic, det_range, width, height, source="", identifier=""):
        """
        Constructor.
        :param topic: The name of the Kafka topic to publish to.
        :param source: The data source to histogram.
        :param det_range: The range of sequential detectors to histogram over.
        :param width: How many detectors in a row.
        :param height:
        :param identifier: An optional identifier for the histogram.
        """
        _validate_parameters(det_range, width, height)
        self._histogram = None
        self.x_edges = None
        self.det_range = det_range
        # The number of bins is the number of detectors.
        self.num_bins = det_range[1] - det_range[0] + 1
        self.width = width
        self.height = height
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source if source.strip() != "" else None

        self._initialise_histogram()

    def _initialise_histogram(self):
        """
        Create a zeroed histogram.
        """
        # Work out the edges for the 2d histogram.
        _, self.x_edges, self.y_edges = np.histogram2d(
            [],
            [],
            range=((0, self.width), (0, self.height)),
            bins=(self.width, self.height),
        )

        # The data is actually stored as a 1d histogram, it is converted to 2d
        # when read - this speeds things up significantly.
        self._histogram, self._edges = np.histogram(
            [], range=self.det_range, bins=(self.det_range[1] - self.det_range[0])
        )

    @property
    def data(self):
        # Create an empty 2d histogram
        hist2d, _, _ = np.histogram2d(
            [],
            [],
            range=((0, self.width), (0, self.height)),
            bins=(self.width, self.height),
        )

        # Copy the data over
        for det_id in range(self.det_range[0], self.det_range[1]):
            x = (det_id - 1) % self.width
            y = ((det_id - 1) // self.width) % self.height
            hist2d[x][y] = self._histogram[det_id - self.det_range[0]]

        return hist2d

    @property
    def shape(self):
        return self.width, self.height

    def add_data(self, pulse_time, tofs, det_ids, source=""):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time.
        :param tofs: The time-of-flight data.
        :param det_ids: The detector data.
        :param source: The source of the event.
        """
        # Discard any messages not from the specified source.
        if self.source is not None and source != self.source:
            return

        self.last_pulse_time = pulse_time

        self._histogram += np.histogram(
            det_ids, range=self.det_range, bins=(self.det_range[1] - self.det_range[0])
        )[0]

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._initialise_histogram()
