import argparse
import json
import numpy as np
from kafka_consumer import Consumer
from event_source import EventSource
from histogrammer2d import Histogrammer2d
from histogrammer1d import Histogrammer1d
from histogram_factory import HistogramFactory


def plot_histogram(hist):
    """
    Plot a histogram.

    :param hist: The histogram to plot.
    """
    import matplotlib.pyplot as plt

    if isinstance(hist, Histogrammer1d):
        width = 0.7 * (hist.x_edges[1] - hist.x_edges[0])
        center = (hist.x_edges[:-1] + hist.x_edges[1:]) / 2
        plt.bar(center, hist.histogram, align="center", width=width)
        plt.show()
    elif isinstance(hist, Histogrammer2d):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        x, y = np.meshgrid(hist.x_edges, hist.y_edges)
        ax.pcolormesh(x, y, hist.histogram)
        plt.show()


def main(brokers, topic, json_config, debug):
    # Extract the configuration from the JSON.
    config = json.loads(json_config)
    # Create the histograms
    histograms = HistogramFactory.generate(config)

    # Initialisation
    config_consumer = Consumer(config["data_brokers"], config["data_topics"])
    event_source = EventSource(config_consumer)

    while True:
        buffs = []

        while len(buffs) == 0:
            buffs = event_source.get_data()

        for hist in histograms:
            for b in buffs:
                x = b["tofs"]
                y = b["det_ids"]
                hist.add_data(x, y)

        if debug:
            # Only plot the first histogram
            plot_histogram(histograms[0])
            # Exit the program when the graph is closed
            return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # TODO:This are commented out for now as we are loading the information from the
    # json config file. When using it for real this will define the broker and
    # topic on which to listen for configuration commands
    # parser.add_argument(
    #     "-b",
    #     "--brokers",
    #     type=str,
    #     nargs="+",
    #     help="the broker addresses",
    #     # required=True,
    # )
    #
    # parser.add_argument(
    #     "-t", "--topic", type=str, help="the information topic", required=True,
    # )

    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="runs the program until it gets some data then plots it",
    )

    # This argument is temporary while we are decided on how to configure the
    # histogrammer via Kafka.
    parser.add_argument(
        "-c", "--config", type=str, help="the configuration as JSON", required=True
    )

    args = parser.parse_args()

    with open(args.config, "r") as f:
        json_str = f.read()

    main(None, None, json_str, args.debug)
