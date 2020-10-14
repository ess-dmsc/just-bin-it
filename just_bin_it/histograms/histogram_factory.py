import logging
import time

from just_bin_it.exceptions import JustBinItException
from just_bin_it.histograms.det_histogram import DetHistogram
from just_bin_it.histograms.histogram1d import Histogram1d
from just_bin_it.histograms.histogram2d import Histogram2d


def parse_config(configuration, current_time=None):
    """
    Parses the configuration that defines the histograms.

    Currently handles both the old-style syntax and the new one (see docs).

    :param configuration: The dictionary containing the configuration.
    :param current_time: The time to use for defining the start time.
    :return: tuple of start time, stop time and the list of histograms
    """
    brokers = configuration["data_brokers"] if "data_brokers" in configuration else None
    topics = configuration["data_topics"] if "data_topics" in configuration else None
    start = configuration["start"] if "start" in configuration else None
    stop = configuration["stop"] if "stop" in configuration else None

    # Interval is configured in seconds but needs to be converted to milliseconds
    interval = (
        configuration["interval"] * 10 ** 3 if "interval" in configuration else None
    )

    if interval and (start or stop):
        raise Exception(
            "Cannot define 'interval' in combination with start and/or stop"
        )

    if interval and interval < 0:
        raise Exception("Interval cannot be negative")

    if interval:
        start = int(current_time) if current_time else int(time.time() * 1000)
        stop = start + interval

    hist_configs = []

    if "histograms" in configuration:
        for hist in configuration["histograms"]:
            if _is_old_style_config(hist):
                _handle_old_style_config(brokers, hist, topics)
            hist_configs.append(hist)

    return start, stop, hist_configs


def _handle_old_style_config(brokers, hist, topics):
    # Old style configs have the brokers and topics defined at the top-level.
    if not brokers or not topics:
        raise Exception("Either the data brokers or data topics were not supplied")
    hist["data_brokers"] = brokers
    hist["data_topics"] = topics


def _is_old_style_config(hist):
    return "data_brokers" not in hist or "data_topics" not in hist


class HistogramFactory:
    @staticmethod
    def generate(configuration):
        """
        Generate the histograms based on the supplied configuration.

        :param configuration: The configuration.
        :return: A list of created histograms.
        """
        histograms = []

        for h in configuration:
            hist = None

            hist_type = h["type"]
            topic = h["topic"]
            num_bins = h["num_bins"] if "num_bins" in h else None
            tof_range = tuple(h["tof_range"]) if "tof_range" in h else None
            det_range = tuple(h["det_range"]) if "det_range" in h else None
            source = h["source"] if "source" in h else ""
            identifier = h["id"] if "id" in h else ""
            width = h["width"] if "width" in h else 512
            height = h["height"] if "height" in h else 512

            try:
                if hist_type == "hist1d":
                    hist = Histogram1d(topic, num_bins, tof_range, det_range, source)
                elif hist_type == "hist2d":
                    hist = Histogram2d(topic, num_bins, tof_range, det_range, source)
                elif hist_type == "dethist":
                    hist = DetHistogram(
                        topic, tof_range, det_range, width, height, source
                    )
                else:
                    # Log but do nothing
                    logging.warning(
                        "Unrecognised histogram type: %s", hist_type
                    )  # pragma: no mutate
            except Exception as error:
                logging.warning(
                    "Could not create histogram. %s", error
                )  # pragma: no mutate

            if hist is not None:
                hist.identifier = identifier
                histograms.append(hist)

        return histograms

    @staticmethod
    def _check_1d_info(num_bins, tof_range, det_range):
        """
        Checks that the required parameters are defined, if not throw.

        :param num_bins: The number of histogram bins.
        :param tof_range: The time-of-flight range.
        :param det_range: The detector range.
        """
        missing = []
        invalid = []

        HistogramFactory._check_tof(tof_range, missing, invalid)
        HistogramFactory._check_bins(num_bins, missing, invalid)

        # det_range is optional
        if det_range:
            HistogramFactory._check_det_range(det_range, missing, invalid)

        if missing or invalid:
            HistogramFactory._generate_exception(missing, invalid, "1D")

    @staticmethod
    def _check_2d_info(num_bins, tof_range, det_range):
        """
        Checks that the required parameters are defined, if not throw.

        :param num_bins: The number of histogram bins.
        :param tof_range: The time-of-flight range.
        :param det_range: The detector range.
        """
        missing = []
        invalid = []

        HistogramFactory._check_tof(tof_range, missing, invalid)
        HistogramFactory._check_det_range(det_range, missing, invalid)
        HistogramFactory._check_bins(num_bins, missing, invalid)
        if missing or invalid:
            HistogramFactory._generate_exception(missing, invalid, "2D")

    @staticmethod
    def _check_2d_map_info(tof_range, det_range, width, height):
        """
        Checks that the required parameters are defined, if not throw.

        :param tof_range: The time-of-flight range.
        :param det_range: The detector range.
        :param width: The detector width.
        :param height: The detector height.
        """
        missing = []
        invalid = []

        HistogramFactory._check_tof(tof_range, missing, invalid)
        HistogramFactory._check_det_range(det_range, missing, invalid)
        HistogramFactory._check_int(width, "width", invalid)
        HistogramFactory._check_int(height, "height", invalid)
        if missing or invalid:
            HistogramFactory._generate_exception(missing, invalid, "2D Map")

    @staticmethod
    def _check_tof(tof_range, missing, invalid):
        if tof_range is None:
            missing.append("TOF range")  # pragma: no mutate
        elif (
            not isinstance(tof_range, list)
            and not isinstance(tof_range, tuple)
            or len(tof_range) != 2
        ):
            invalid.append("TOF range")  # pragma: no mutate

    @staticmethod
    def _check_bins(num_bins, missing, invalid):
        if num_bins is None:
            missing.append("number of bins")  # pragma: no mutate
        elif not isinstance(num_bins, int):
            HistogramFactory._check_int(num_bins, "number of bins", invalid)

    @staticmethod
    def _check_int(value, field, invalid):
        if not isinstance(value, int):
            invalid.append(field)  # pragma: no mutate

    @staticmethod
    def _check_det_range(det_range, missing, invalid):
        if det_range is None:
            missing.append("Detector range")  # pragma: no mutate
        elif (
            not isinstance(det_range, list)
            and not isinstance(det_range, tuple)
            or len(det_range) != 2
        ):
            invalid.append("Detector range")  # pragma: no mutate

    @staticmethod
    def _generate_exception(missing, invalid, hist_type):
        error_msg = ""

        if missing:
            error_msg += (
                f"Missing information for {hist_type} histogram: {', '.join(missing)}"
            )  # pragma: no mutate
        if invalid:
            error_msg += (
                f"Invalid information for {hist_type} histogram:  {', '.join(missing)}"
            )  # pragma: no mutate
        if error_msg:
            raise JustBinItException(error_msg)
