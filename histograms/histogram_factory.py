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

            if h["type"] == "hist1d":
                hist = Histogrammer1d(h["topic"], h["num_bins"], tuple(h["tof_range"]))
            elif h["type"] == "hist2d":
                hist = Histogrammer2d(
                    h["topic"],
                    h["num_bins"],
                    tuple(h["tof_range"]),
                    tuple(h["det_range"]),
                )
            elif h["type"] == "sehist1d":
                hist = SingleEventHistogrammer1d(
                    h["topic"], h["num_bins"], tuple(h["tof_range"])
                )
            else:
                # TODO: skip it, throw or what?
                print(f"Unrecognised histogram type: {h['type']}")
                pass

            if hist is not None:
                histograms.append(hist)

        return histograms
