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

TOF_2D_TYPE = "hist2d"


def validate_hist_2d(histogram_config):
    required = [
        "tof_range",
        "num_bins",
        "topic",
        "data_topics",
        "data_brokers",
        "det_range",
        "type",
    ]
    if any(req not in histogram_config for req in required):
        return False

    if histogram_config["type"] != TOF_2D_TYPE:
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

    if not check_det_range(histogram_config["det_range"]):
        return False

    if "id" in histogram_config and not check_id(histogram_config["id"]):
        return False

    if "source" in histogram_config and not check_source(histogram_config["source"]):
        return False

    return True


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

        self._initialise_histogram()

    def _initialise_histogram(self):
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
        self._initialise_histogram()
