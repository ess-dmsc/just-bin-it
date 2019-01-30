import argparse
import numpy as np
from time import sleep
from endpoints.kafka_consumer import Consumer
from endpoints.kafka_producer import Producer
from endpoints.event_source import EventSource
from histograms.histogrammer2d import Histogrammer2d
from histograms.histogrammer1d import Histogrammer1d
from histograms.histogram_factory import HistogramFactory
from endpoints.config_source import ConfigSource
from endpoints.histogram_sink import HistogramSink


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


def main(brokers, topic, one_shot):
    """
    The main execution function.

    :param brokers: The brokers to listen for the configuration commands on.
    :param topic: The topic to listen for commands on.
    :param one_shot: Run in one-shot mode.
    """
    # Create the config listener
    config_consumer = Consumer(brokers, [topic])
    config_source = ConfigSource(config_consumer)

    event_source = None
    hist_sink = None
    histograms = []

    while True:
        sleep(0.5)

        # Check for a configuration change
        config = config_source.get_new_config()

        if config is not None:
            # Configure the event source and create the histograms
            consumer = Consumer(config["data_brokers"], config["data_topics"])
            producer = Producer(config["data_brokers"])
            event_source = EventSource(consumer)
            hist_sink = HistogramSink(producer)
            histograms = HistogramFactory.generate(config)

        if event_source is None:
            # No event source means we are waiting for a configuration
            continue

        buffs = []

        while len(buffs) == 0:
            buffs = event_source.get_data()

        for hist in histograms:
            for b in buffs:
                x = b["tofs"]
                y = b["det_ids"]
                hist.add_data(x, y)

        if one_shot:
            # Only plot the first histogram
            plot_histogram(histograms[0])
            # Exit the program when the graph is closed
            return
        else:
            # Publish histogram data
            for h in histograms:
                hist_sink.send_histogram(h.topic, h)


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
        "-t", "--topic", type=str, help="the configuration topic", required=True
    )

    parser.add_argument(
        "-o",
        "--one-shot-plot",
        action="store_true",
        help="runs the program until it gets some data, plot it and then exit."
        " Used for testing",
    )

    args = parser.parse_args()
    main(args.brokers, args.topic, args.one_shot_plot)
