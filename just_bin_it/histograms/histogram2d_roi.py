import logging
import numbers

import numpy as np

from just_bin_it.histograms.binning import accumulate_counts
from just_bin_it.histograms.input_validators import (
    check_data_brokers,
    check_data_topics,
    check_id,
    check_source,
    check_topic,
    is_collection_numeric,
)

ROI_TYPE = "roihist"


def validate_hist_2d_roi(histogram_config):
    required = ["topic", "data_topics", "data_brokers", "width", "left_edges", "type"]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != ROI_TYPE:
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    if not check_data_topics(histogram_config["data_topics"]):
        return False

    if not check_data_brokers(histogram_config["data_brokers"]):
        return False

    if (
        not isinstance(histogram_config["width"], numbers.Number)
        or histogram_config["width"] < 1
    ):
        return False

    if (
        not isinstance(histogram_config["left_edges"], list)
        or len(histogram_config["left_edges"]) == 0
        or not is_collection_numeric(histogram_config["left_edges"])
    ):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


class RoiHistogram:
    """Two dimensional histogram for a region of interest."""

    def __init__(self, topic, left_edges, width, source="", identifier=""):
        """
        Constructor.
        :param topic: The name of the Kafka topic to publish to.
        :param left_edges:
        :param width: How many detectors in a row.
        :param source: The data source to histogram.
        :param identifier: An optional identifier for the histogram.
        """
        self._histogram = None
        self.x_edges = [x for x in range(width)]
        self.y_edges = [y for y in range(len(left_edges))]
        self.left_edges = left_edges
        self._row_starts = np.asarray(left_edges)
        if np.any(np.diff(self._row_starts.astype(np.float64)) < width):
            raise ValueError("ROI rows must be ordered and must not overlap")
        self.width = width
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source if source.strip() != "" else None

        self._create_empty_histogram()

    def _create_empty_histogram(self):
        self._histogram = np.zeros((self.width, len(self.left_edges)))

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
        included = (det_ids >= self._row_starts[0]) & (
            det_ids < self._row_starts[-1] + self.width
        )
        det_ids = det_ids[included]
        rows = np.searchsorted(self._row_starts, det_ids, side="right") - 1
        columns = det_ids.astype(np.float64) - self._row_starts[rows]
        included = columns < self.width
        indices = columns[included].astype(np.intp) * self.shape[1] + rows[included]
        accumulate_counts(self._histogram, indices)

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._histogram.fill(0)

    def counts_sum(self):
        return self._histogram.sum()
