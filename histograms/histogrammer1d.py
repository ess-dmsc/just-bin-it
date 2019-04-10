import numpy as np
import numpy.ma as ma
from fast_histogram import histogram1d


class Histogrammer1d:
    """Histograms time-of-flight for a range of detectors into a 1-D histogram."""

    def __init__(
        self,
        topic,
        num_bins=50,
        tof_range=None,
        source=None,
        preprocessor=None,
        roi=None,
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
        """
        self.histogram = None
        self.x_edges = None
        self.tof_range = tof_range
        self.num_bins = num_bins
        self.source = source
        self.topic = topic
        self.preprocessor = preprocessor
        self.roi = roi

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

        if self.preprocessor is not None:
            pulse_time, tofs, det_ids = self._preprocess_data(pulse_time, tofs, det_ids)

        if self.roi is not None:
            mask = self._get_mask(pulse_time, tofs, det_ids)
            if mask:
                tofs = ma.array(tofs, mask=mask).compressed()

        if self.histogram is None:
            # If no tof range defined then guess one
            if self.tof_range is None:
                self.tof_range = (0, max(tofs))

            # Assumes that fast_histogram produces the same bins as numpy.
            self.x_edges = np.histogram_bin_edges(tofs, self.num_bins, self.tof_range)
            self.histogram = histogram1d(tofs, range=self.tof_range, bins=self.num_bins)
        else:
            self.histogram += histogram1d(
                tofs, range=self.tof_range, bins=self.num_bins
            )

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
            # TODO: log
            print("Exception while preprocessing data")
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
            # TODO: log
            print("Exception while try to check ROI")
            mask = None
        return mask
