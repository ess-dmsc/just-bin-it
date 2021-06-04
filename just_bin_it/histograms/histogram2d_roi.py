import logging
import numbers

import numpy as np

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
        self.mask = []
        self.bins = []
        self.left_edges = left_edges
        self.width = width
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source if source.strip() != "" else None

        self._initialise_histogram()

    def _initialise_histogram(self):
        """
        Create a zeroed histogram.
        """
        self.bins.clear()
        self.mask.clear()

        # Work out the bins
        for i, edge in enumerate(self.left_edges):
            self.bins.extend([edge + x for x in range(self.width)])
            self.mask.extend([0 for _ in range(self.width)])
            if i < len(self.left_edges) - 1 and self._is_roi_discontiguous(
                self.bins[~0], self.left_edges[i + 1]
            ):
                # Add extra bin for ids we don't care about between the end of
                # this row and the start of the next
                self.bins.append(self.bins[~0] + 1)
                self.mask.append(1)

        # TODO: put this information in a doc?
        # numpy includes the right most edge of the last bin as part of that bin
        # e.g., bins = [1,2,3] gives two buckets 1 to 1.99999 and 2 to 3
        # What we want is three buckets one each for 1, 2, 3, so we add an extra
        # bin, e.g., bins = [1,2,3,4] but this means the value 4 will end up in
        # the "3" bin.
        # To avoid this we add one more bin and just ignore the two "extra" bins
        # e.g., bins = [1,2,3,4,5]
        self.bins.append(self.bins[~0] + 1)
        self.bins.append(self.bins[~0] + 1)
        self.mask.append(1)
        self.mask.append(1)

        # The data is actually stored as a 1d histogram, it is converted to 2d
        # when read - this speeds things up significantly.
        self._histogram, _ = np.histogram([], bins=self.bins)

    def _is_roi_discontiguous(self, last_bin, next_left_edge):
        return next_left_edge != last_bin + 1

    @property
    def data(self):
        hist2d, _, _ = np.histogram2d([], [], bins=self.shape)
        i = 0
        for mask, value in zip(self.mask, self._histogram):
            if not mask:
                x = i % self.width
                y = i // self.width
                hist2d[x][y] = value
                i += 1
        return hist2d

    @property
    def shape(self):
        return self.width, len(self.left_edges)

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

        self._histogram += np.histogram(det_ids, bins=self.bins)[0]

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._initialise_histogram()
