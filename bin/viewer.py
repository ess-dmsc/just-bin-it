import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.sources import HistogramSource
from just_bin_it.utilities.plotter import plot_histograms


def convert_for_plotting(histogram):
    """
    Convert histogram data to a form for plotting.

    :param histogram: The histogram to convert.
    """

    class Histogram:
        pass

    h = Histogram()

    if len(histogram["dims"]) == 1:
        # 1-D
        h.x_edges = histogram["dims"][0]["edges"]
    else:
        # 2-D
        h.x_edges = histogram["dims"][0]["edges"]
        h.y_edges = histogram["dims"][1]["edges"]

    h.data = histogram["data"]

    return [h]


def main(brokers, topic):
    """

    :param brokers: The brokers to listen for data on.
    :param topic: The topic to listen for data on.
    """
    # Create the listener
    hist_consumer = Consumer(brokers, [topic])
    hist_source = HistogramSource(hist_consumer)

    buffs = []

    while len(buffs) == 0:
        buffs = hist_source.get_new_data()

    # Only care about the most recent histogram and don't care about kafka timestamps
    _, _, hist_data = buffs[-1]

    hists = convert_for_plotting(hist_data)
    plot_histograms(hists)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-b",
        "--brokers",
        type=str,
        nargs="+",
        help="the broker addresses",
        required=True,
    )

    required_args.add_argument(
        "-t", "--topic", type=str, help="the histogram data topic", required=True
    )

    args = parser.parse_args()
    main(args.brokers, args.topic)
