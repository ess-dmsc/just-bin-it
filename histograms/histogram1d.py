import numpy as np
import numpy.ma as ma
import logging
from fast_histogram import histogram1d


class Histogram1d:
    """One dimensional histogram for time-of-flight."""

    def __init__(
        self,
        topic,
        num_bins,
        tof_range,
        source=None,
        preprocessor=None,
        roi=None,
        id="",
    ):
        """
        Constructor.

        Note on preprocessing functions: this should used for relatively low impact
        processing, i.e. avoid CPU intense algorithms.

        :param topic: The name of the Kafka topic to publish to.
        :param num_bins: The number of bins to divide the time-of-flight up into.
        :param tof_range: The time-of-flight range to histogram over.
        :param source: The data source to histogram.
        :param preprocessor: The function to apply to the data before adding.
        :param roi: The function for checking data is within the region of interest.
        :param id: An optional identifier for the histogram.
        """
        self._histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.num_bins = num_bins
        self.source = source
        self.topic = topic
        self.preprocessor = preprocessor
        self.roi = roi
        self.last_pulse_time = 0
        self.id = id

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

        if self.preprocessor is not None:
            pulse_time, tofs, det_ids = self._preprocess_data(pulse_time, tofs, det_ids)

        if self.roi is not None:
            mask = self._get_mask(pulse_time, tofs, det_ids)
            if mask:
                tofs = ma.array(tofs, mask=mask).compressed()

        self._histogram += histogram1d(tofs, range=self.tof_range, bins=self.num_bins)

    @property
    def data(self):
        return self._histogram

    @property
    def shape(self):
        return self._histogram.shape

    def _preprocess_data(self, pulse_time, tofs, det_ids):
        """
        Apply the defined processing function to the data.

        :param pulse_time: The pulse time.
        :param tofs: The time-of-flight data.
        :param det_ids: The detector ids.
        :return: The newly processed data.
        """
        try:
            pulse_time, tofs, det_ids = self.preprocessor(pulse_time, tofs, det_ids)
        except Exception:
            logging.warning("Exception while preprocessing data")
        return pulse_time, tofs, det_ids

    def _get_mask(self, pulse_time, tofs, det_ids):
        """
        Apply the defined processing function to the data to generate a mask.

        1 is used to indicate a masked value.
        0 is used to indicate an unmasked value.

        :param pulse_time: The pulse time.
        :param tofs: The time-of-flight data.
        :param det_ids: The detector ids.
        :return: The newly processed data.
        """
        try:
            mask = self.roi(pulse_time, tofs, det_ids)
        except Exception:
            logging.warning("Exception while try to check ROI")
            mask = None
        return mask

    def clear_data(self):
        """
        Clears the histogram data, but maintains the other values (e.g. edges etc.)
        """
        logging.info("Clearing data")  # pragma: no mutate
        self._histogram = histogram1d([], range=self.tof_range, bins=self.num_bins)