import logging
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d
from histograms.single_event_histogrammer1d import SingleEventHistogrammer1d


class HistogramFactory:
    @staticmethod
    def generate(configuration):
        """
        Generate the histograms based on the supplied configuration.

        :param configuration: The configuration.
        :return: A list of created histograms.
        """
        histograms = []

        for h in configuration["histograms"]:
            hist = None

            hist_type = h["type"]
            topic = h["topic"]
            num_bins = h["num_bins"] if "num_bins" in h else None
            tof_range = tuple(h["tof_range"]) if "tof_range" in h else None
            det_range = tuple(h["det_range"]) if "det_range" in h else None
            source = h["source"] if "source" in h else None

            if hist_type == "hist1d":
                HistogramFactory.check_1d_info(num_bins, tof_range)
                hist = Histogrammer1d(topic, num_bins, tof_range, source)
            elif hist_type == "hist2d":
                hist = Histogrammer2d(
                    topic,
                    num_bins,
                    tof_range,
                    det_range,
                    # source,
                )
            elif hist_type == "sehist1d":
                hist = SingleEventHistogrammer1d(topic, num_bins, tof_range, source)
            else:
                # TODO: skip it, throw or what?
                logging.warning(
                    f"Unrecognised histogram type: {hist_type}"
                )  # pragma: no mutate
                pass

            if hist is not None:
                histograms.append(hist)

        return histograms

    @staticmethod
    def check_1d_info(num_bins, tof_range):
        missing = []

        if tof_range is None:
            missing.append("TOF range")  # pragma: no mutate
        if num_bins is None:
            missing.append("number of bins")  # pragma: no mutate

        if missing:
            error_msg = f"Data missing for 1d histogram: {', '.join(missing)}"
            raise Exception(error_msg)
