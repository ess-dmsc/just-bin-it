import logging

import numpy as np
from fast_histogram import histogram2d as fast_histogram2d

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

        self._histogram += self._histogram_tof_detector(tof, det_ids)

    def _histogram_tof_detector(self, tof, det_ids):
        tof = np.asarray(tof)
        det_ids = np.asarray(det_ids)
        histogram = fast_histogram2d(
            tof,
            det_ids,
            range=(self.tof_range, self.det_range),
            bins=self.num_bins,
        )
        self._add_upper_edge_counts(histogram, tof, det_ids)
        return histogram

    def _add_upper_edge_counts(self, histogram, tof, det_ids):
        tof_bins, det_bins = self.shape
        tof_min, tof_max = self.tof_range
        det_min, det_max = self.det_range

        on_tof_max = (tof == tof_max) & (det_ids >= det_min) & (det_ids <= det_max)
        if np.any(on_tof_max):
            det_indices = self._bin_indices(det_ids[on_tof_max], self.det_range, det_bins)
            tof_indices = np.full(det_indices.shape, tof_bins - 1)
            np.add.at(histogram, (tof_indices, det_indices), 1)

        on_det_max = (det_ids == det_max) & (tof >= tof_min) & (tof < tof_max)
        if np.any(on_det_max):
            tof_indices = self._bin_indices(tof[on_det_max], self.tof_range, tof_bins)
            det_indices = np.full(tof_indices.shape, det_bins - 1)
            np.add.at(histogram, (tof_indices, det_indices), 1)

    def _bin_indices(self, values, value_range, num_bins):
        value_min, value_max = value_range
        indices = np.floor((values - value_min) * num_bins / (value_max - value_min))
        return np.clip(indices.astype(np.intp), 0, num_bins - 1)

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

    def counts_sum(self):
        return self._histogram.sum()
