import argparse
import numpy as np
from endpoints.kafka_consumer import Consumer
from endpoints.sources import HistogramSource


def plot_histogram(hist):
    """
    Plot a histogram.

    :param hist: The histogram to plot.
    """
    import matplotlib

    matplotlib.use("TkAgg")
    from matplotlib import pyplot as plt

    n_dims = len(hist["dims"])

    if n_dims == 1:
        edges = hist["dims"][0]["edges"]
        data = hist["data"]
        width = 0.8 * (edges[1] - edges[0])
        plt.bar(edges[:-1], data, align="edge", width=width)
        plt.show()
    elif n_dims == 2:
        x_edges = hist["dims"][0]["edges"]
        y_edges = hist["dims"][1]["edges"]
        data = hist["data"].T

        fig = plt.figure()
        ax = fig.add_subplot(111)
        x, y = np.meshgrid(x_edges, y_edges)
        ax.pcolormesh(x, y, data)
        plt.show()


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
    _, _, hist = buffs[-1]

    plot_histogram(hist)


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
