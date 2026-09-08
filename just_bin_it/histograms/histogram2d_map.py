import logging
import numbers

import numpy as np

from just_bin_it.histograms.binning import accumulate_counts
from just_bin_it.histograms.input_validators import (
    check_data_brokers,
    check_data_topics,
    check_det_range,
    check_id,
    check_source,
    check_topic,
)

MAP_TYPE = "dethist"


def validate_hist_2d_map(histogram_config):
    required = [
        "topic",
        "data_topics",
        "data_brokers",
        "det_range",
        "width",
        "height",
        "type",
    ]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != MAP_TYPE:
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    if not check_data_topics(histogram_config["data_topics"]):
        return False

    if not check_data_brokers(histogram_config["data_brokers"]):
        return False

    if not check_det_range(histogram_config["det_range"]):
        return False

    if (
        not isinstance(histogram_config["height"], numbers.Number)
        or histogram_config["height"] < 1
    ):
        return False

    if (
        not isinstance(histogram_config["width"], numbers.Number)
        or histogram_config["width"] < 1
    ):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


class DetHistogram:
    """Two dimensional histogram for detectors."""

    def __init__(self, topic, det_range, width, height, source="", identifier=""):
        """
        Constructor.
        :param topic: The name of the Kafka topic to publish to.
        :param det_range: The range of sequential detectors to histogram over.
        :param width: How many detectors in a row.
        :param height: How many rows of detectors.
        :param source: The data source to histogram.
        :param identifier: An optional identifier for the histogram.
        """
        self._histogram = None
        self.x_edges = None
        self.y_edges = None
        # Second det range value is calculated, thus the user value is ignored.
        self.det_range = (det_range[0], det_range[0] + width * height)
        # The number of bins is the number of detectors.
        self.num_bins = width * height
        self.width = width
        self.height = height
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source if source.strip() != "" else None

        self._initialise_histogram()

    def _initialise_histogram(self):
        self._calculate_edges()
        self._create_empty_histogram()

    def _create_empty_histogram(self):
        # Store output in (x, y) order; width/height retain the input geometry.
        self._histogram = np.zeros((self.width, self.height))

    def _calculate_edges(self):
        self.x_edges = np.arange(self.width + 1, dtype=np.float64)
        self.y_edges = np.arange(self.height + 1, dtype=np.float64)

    @property
    def data(self):
        return self._histogram.copy()

    @property
    def shape(self):
        return self._histogram.shape

    def add_data(self, pulse_time, tofs, det_ids, source=""):
        """
        Add data to the histogram.

        :param pulse_time: The pulse time.
        :param tofs: Not used.
        :param det_ids: The detector data.
        :param source: The source of the event.
        """
        # Discard any messages not from the specified source.
        if self.source is not None and source != self.source:
            return

        self.last_pulse_time = pulse_time

        det_ids = np.asarray(det_ids).ravel()
        included = (det_ids >= self.det_range[0]) & (det_ids < self.det_range[1])
        # Filter before converting, and subtract before rounding fractional IDs.
        pixels = (det_ids[included].astype(np.float64) - self.det_range[0]).astype(
            np.intp
        )
        x, y = pixels % self.width, pixels // self.width
        accumulate_counts(self._histogram, x * self.shape[1] + y)

    def add_binned_data(self, pulse_time, binned_data, source=""):
        """
        Add pre-binned detector data to the histogram.

        :param pulse_time: The pulse time.
        :param binned_data: The pre-binned data.
        :param source: The source of the event.
        """
        # Discard any messages not from the specified source.
        if self.source is not None and source != self.source:
            return

        counts = binned_data.counts.sum(axis=0)
        if counts.size != self.num_bins:
            logging.warning(
                "Skipping binned detector histogram data with %s pixels, expected %s",
                counts.size,
                self.num_bins,
            )
            return

        self.last_pulse_time = pulse_time

        if np.issubdtype(self._histogram.dtype, np.integer) and np.issubdtype(
            counts.dtype, np.floating
        ):
            self._histogram = self._histogram.astype(counts.dtype)

        self._histogram += counts.reshape((self.height, self.width)).T

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._histogram.fill(0)

    def counts_sum(self):
        return self._histogram.sum()
