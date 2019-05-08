import argparse
import os
import json
import logging
import numpy as np
import time
from endpoints.kafka_consumer import Consumer
from endpoints.kafka_producer import Producer
from histograms.histogrammer2d import Histogrammer2d
from histograms.histogrammer1d import Histogrammer1d  # NOQA
from histograms.single_event_histogrammer1d import SingleEventHistogrammer1d  # NOQA
from histograms.histogram_factory import HistogramFactory
from endpoints.sources import ConfigSource, EventSource
from endpoints.histogram_sink import HistogramSink


def plot_histogram(hist):
    """
    Plot a histogram.

    :param hist: The histogram to plot.
    """
    import matplotlib

    matplotlib.use("TkAgg")
    from matplotlib import pyplot as plt

    if isinstance(hist, Histogrammer2d):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        x, y = np.meshgrid(hist.x_edges, hist.y_edges)
        ax.pcolormesh(x, y, hist.histogram)
        plt.show()
    else:
        width = 0.7 * (hist.x_edges[1] - hist.x_edges[0])
        center = (hist.x_edges[:-1] + hist.x_edges[1:]) / 2
        plt.bar(center, hist.histogram, align="center", width=width)
        plt.show()


def load_config_file(file):
    """
    Load the configuration file, if present.

    :param file: The file path.
    :return: The extracted data as JSON.
    """
    try:
        path = os.path.abspath(file)
        with open(path, "r") as f:
            data = f.read()

        return json.loads(data)
    except Exception as error:
        raise Exception("Could not load configuration file") from error


def configure_histogramming(config):
    """
    Configure histogramming based on the supplied configuration.

    :param config: The configuration.
    :return: A tuple of the event source, the histogram sink and the histograms.
    """
    consumer = Consumer(config["data_brokers"], config["data_topics"])
    producer = Producer(config["data_brokers"])
    event_source = EventSource(consumer)
    hist_sink = HistogramSink(producer)
    histograms = HistogramFactory.generate(config)

    return event_source, hist_sink, histograms


def run_simulation(brokers, one_shot):
    producer = Producer(brokers)
    hist_sink = HistogramSink(producer)
    hist = Histogrammer1d(topic="jbi_sim_data", tof_range=(0, 10000))

    while True:
        time.sleep(1)

        # Generate gaussian data centred around 3000
        tofs = np.random.normal(3000, 1000, 1000)
        hist.add_data(time.time_ns(), tofs)

        if one_shot:
            plot_histogram(hist)
            # Exit the program when the graph is closed
            return
        else:
            # Publish histogram data
            hist_sink.send_histogram(hist.topic, hist)


def main(brokers, topic, one_shot, simulation, initial_config=None):
    """
    The main execution function.

    :param brokers: The brokers to listen for the configuration commands on.
    :param topic: The topic to listen for commands on.
    :param one_shot: Run in one-shot mode.
    :param simulation: Run in simulation mode.
    :param initial_config: A histogram configuration to start with.
    """
    if simulation:
        logging.info("Running in simulation mode")
        run_simulation(brokers, one_shot)
        return

    # Create the config listener
    logging.info("Creating configuration consumer")
    config_consumer = Consumer(brokers, [topic])
    config_source = ConfigSource(config_consumer)

    event_source = None
    hist_sink = None
    histograms = []

    if initial_config:
        # Create the histograms based on the supplied configuration
        event_source, hist_sink, histograms = configure_histogramming(initial_config)

    while True:
        time.sleep(0.5)

        # Check for a configuration message
        config_msg = config_source.get_new_data()

        if len(config_msg) > 0:
            # We are only interested in the "latest" message
            logging.info("New configuration message received")
            msg = config_msg[-1]

            if msg["cmd"] == "restart":
                for hist in histograms:
                    hist.clear_data()
            elif msg["cmd"] == "config":
                event_source, hist_sink, histograms = configure_histogramming(msg)
            else:
                logging.warning(f'Unknown command received: {msg["cmd"]}')

        if event_source is None:
            # No event source means we are waiting for a configuration
            continue

        buffs = []

        while len(buffs) == 0:
            buffs = event_source.get_new_data()

        for hist in histograms:
            for b in buffs:
                pt = b["pulse_time"]
                x = b["tofs"]
                y = b["det_ids"]
                src = b["source"]
                hist.add_data(pt, x, y, src)

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
    logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

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
        "-c",
        "--config-file",
        type=str,
        help="configure an inital histogram from a JSON file",
    )

    parser.add_argument(
        "-o",
        "--one-shot-plot",
        action="store_true",
        help="runs the program until it gets some data, plot it and then exit."
        " Used for testing",
    )

    parser.add_argument(
        "-s",
        "--simulation-mode",
        action="store_true",
        help="runs the program in simulation mode. 1-D histogram data is written"
        " to jbi_sim_data. The configuration topic is ignored",
    )

    args = parser.parse_args()

    init_hist_json = None
    if args.config_file:
        init_hist_json = load_config_file(args.config_file)

    main(
        args.brokers,
        args.topic,
        args.one_shot_plot,
        args.simulation_mode,
        init_hist_json,
    )
