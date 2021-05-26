import logging
import time

from just_bin_it.histograms.histogram1d import (
    TOF_1D_TYPE,
    Histogram1d,
    validate_hist_1d,
)
from just_bin_it.histograms.histogram2d import TOF_2D_TYPE, Histogram2d
from just_bin_it.histograms.histogram2d_map import MAP_TYPE, DetHistogram
from tests.test_config_validation import validate_hist_2d, validate_hist_2d_map


def parse_config(configuration, current_time_ms=None):
    """
    Parses the configuration that defines the histograms.

    :param configuration: The dictionary containing the configuration.
    :param current_time_ms: The time to use for defining the start time (milliseconds).
    :return: tuple of start time, stop time and the list of histograms
    """
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
        start = int(current_time_ms) if current_time_ms else int(time.time() * 1000)
        stop = start + interval

    hist_configs = []

    if "histograms" in configuration:
        for hist in configuration["histograms"]:
            if hist["type"] == TOF_1D_TYPE:
                if not validate_hist_1d(hist):
                    raise Exception("Could not parse 1d histogram")
            elif hist["type"] == TOF_2D_TYPE:
                if not validate_hist_2d(hist):
                    raise Exception("Could not parse 2d histogram")
            elif hist["type"] == MAP_TYPE:
                if not validate_hist_2d_map(hist):
                    raise Exception("Could not parse 2d map")
            else:
                raise Exception("Unexpected histogram type")
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

        for config in configuration:
            histogram = None

            hist_type = config["type"]
            topic = config["topic"]
            num_bins = config["num_bins"] if "num_bins" in config else None
            tof_range = tuple(config["tof_range"]) if "tof_range" in config else None
            det_range = tuple(config["det_range"]) if "det_range" in config else None
            source = config["source"] if "source" in config else ""
            identifier = config["id"] if "id" in config else ""

            try:
                if hist_type == TOF_1D_TYPE:
                    histogram = Histogram1d(
                        topic, num_bins, tof_range, det_range, source, identifier
                    )
                elif hist_type == TOF_2D_TYPE:
                    histogram = Histogram2d(
                        topic, num_bins, tof_range, det_range, source, identifier
                    )
                elif hist_type == MAP_TYPE:
                    width = config["width"] if "width" in config else 512
                    height = config["height"] if "height" in config else 512
                    histogram = DetHistogram(
                        topic, det_range, width, height, source, identifier
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

            if histogram is not None:
                histogram.identifier = identifier
                histograms.append(histogram)

        return histograms
