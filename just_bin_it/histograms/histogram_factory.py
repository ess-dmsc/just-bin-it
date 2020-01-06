import logging
import time
from just_bin_it.histograms.det_histogram import DetHistogram
from just_bin_it.histograms.histogram1d import Histogram1d
from just_bin_it.histograms.histogram2d import Histogram2d
from just_bin_it.histograms.single_event_histogram1d import SingleEventHistogram1d


def parse_config(configuration, current_time=None):
    brokers = configuration["data_brokers"]
    topics = configuration["data_topics"]
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
            hist["data_brokers"] = brokers
            hist["data_topics"] = topics
            hist_configs.append(hist)

    return start, stop, hist_configs


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
                    HistogramFactory.check_1d_info(num_bins, tof_range)
                    hist = Histogram1d(topic, num_bins, tof_range, det_range, source)
                elif hist_type == "hist2d":
                    # TODO: check 2d info
                    hist = Histogram2d(topic, num_bins, tof_range, det_range, source)
                elif hist_type == "sehist1d":
                    hist = SingleEventHistogram1d(topic, num_bins, tof_range, source)
                elif hist_type == "dethist":
                    # TODO: check 2d info
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
    def check_1d_info(num_bins, tof_range):
        """
        Checks that the required parameters are defined, if not throw.

        :param num_bins: The number of histogram bins.
        :param tof_range: The time-of-flight range.
        """
        missing = []

        if tof_range is None:
            missing.append("TOF range")  # pragma: no mutate
        if num_bins is None:
            missing.append("number of bins")  # pragma: no mutate

        # TODO: if det_range supplied check is valid

        if missing:
            error_msg = (
                f"Invalid/missing information for 1d histogram: {', '.join(missing)}"
            )  # pragma: no mutate
            raise Exception(error_msg)
