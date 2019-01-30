from histograms.histogrammer1d import Histogrammer1d
from histograms.histogrammer2d import Histogrammer2d


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

            if h["num_dims"] == 1:
                range = tuple(h["det_range"])
                hist = Histogrammer1d(range, h["num_bins"], h["topic"])
            elif h["num_dims"] == 2:
                hist = Histogrammer2d(
                    tuple(h["tof_range"]),
                    tuple(h["det_range"]),
                    h["num_bins"],
                    h["topic"],
                )
            else:
                # TODO: skip it, throw or what?
                pass

            if hist is not None:
                histograms.append(hist)

        return histograms
