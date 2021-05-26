import logging

import numpy as np

from just_bin_it.histograms.input_validators import (
    check_bins,
    check_data_brokers,
    check_data_topics,
    check_det_range,
    check_id,
    check_source,
    check_tof,
    check_topic,
)

TOF_1D_TYPE = "hist1d"


def validate_hist_1d(histogram_config):
    required = ["tof_range", "num_bins", "topic", "data_topics", "data_brokers", "type"]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != TOF_1D_TYPE:
        return False

    if not check_tof(histogram_config["tof_range"]):
        return False

    if not check_bins(histogram_config["num_bins"]):
        return False

    if not check_topic(histogram_config["topic"]):
        return False

    if not check_data_topics(histogram_config["data_topics"]):
        return False

    if not check_data_brokers(histogram_config["data_brokers"]):
        return False

    if "det_range" in histogram_config and not check_det_range(
        histogram_config["det_range"]
    ):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


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
        self._histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.det_range = det_range
        self.num_bins = num_bins
        self.topic = topic
        self.last_pulse_time = 0
        self.identifier = identifier
        self.source = source if source.strip() != "" else None

        self._initialise_histogram()

    def _initialise_histogram(self):
        """
        Create a zeroed histogram with the correct shape.
        """
        self._histogram, self.x_edges = np.histogram(
            [], range=self.tof_range, bins=self.num_bins
        )

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
            self._histogram = self._histogram + histogram.sum(1)
        else:
            self._histogram += np.histogram(
                tofs, range=self.tof_range, bins=self.num_bins
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
        self._initialise_histogram()
