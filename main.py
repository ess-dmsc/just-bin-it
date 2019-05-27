import argparse
import os
import json
import logging
import numpy as np
import time
from endpoints.kafka_consumer import Consumer
from endpoints.kafka_producer import Producer
from endpoints.kafka_tools import kafka_settings_valid
from histograms.histogrammer2d import Histogrammer2d
from histograms.histogrammer1d import Histogrammer1d  # NOQA
from histograms.single_event_histogrammer1d import SingleEventHistogrammer1d  # NOQA
from histograms.histogram_factory import HistogramFactory
from endpoints.sources import ConfigSource, EventSource, SimulatedEventSource1D
from endpoints.histogram_sink import HistogramSink


class ConfigListener:
    def __init__(self, brokers, config_topic):
        self.brokers = brokers
        self.config_topic = config_topic
        self.config_source = None
        self.message = None

    def connect(self):
        logging.info("Creating configuration consumer")
        while not kafka_settings_valid(self.brokers, [self.config_topic]):
            logging.error(
                f"Could not connect to Kafka brokers or topic for configuration - will retry shortly"
            )
            time.sleep(5)
        config_consumer = Consumer(self.brokers, [self.config_topic])
        self.config_source = ConfigSource(config_consumer)

    def check_for_messages(self):
        """
        Checks for unconsumed messages.

        :return: True if there is a message available.
        """
        messages = self.config_source.get_new_data()
        if messages:
            # Only interested in the "latest" message
            self.message = messages[-1]
            logging.info("New configuration message received")
            return True
        else:
            # Return whether there is a message waiting
            return self.message is not None

    def consume_message(self):
        """
        :return: The waiting message.
        """
        if self.message:
            msg = self.message
            self.message = None
            return msg
        else:
            raise Exception("No message available")


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


class Main:
    def __init__(
        self, brokers, config_topic, one_shot, simulation, initial_config=None
    ):
        """
        The main execution function.

        :param brokers: The brokers to listen for the configuration commands on.
        :param config_topic: The topic to listen for commands on.
        :param one_shot: Histogram some data then plot it. Does not publish results.
        :param simulation: Run in simulation mode.
        :param initial_config: A histogram configuration to start with.
        """
        self.event_source = None
        self.hist_sink = None
        self.histograms = []

        if simulation:
            logging.info("RUNNING IN SIMULATION MODE")

        # Blocks until it connects to the topic
        config_listener = ConfigListener(brokers, config_topic)
        config_listener.connect()

        while True:
            # Handle configuration messages
            if initial_config or config_listener.check_for_messages():
                if initial_config:
                    # If initial configuration supplied, use it only once.
                    msg = initial_config
                    initial_config = None
                else:
                    msg = config_listener.consume_message()

                try:
                    self.handle_command_message(msg, simulation)
                    if not one_shot:
                        # Publish initial empty histograms.
                        self.publish_histograms()
                except Exception as error:
                    logging.error(f"Could not handle configuration: {error}")

            if self.event_source is None:
                # No event source means we are waiting for a configuration.
                continue

            event_buffer = []

            while len(event_buffer) == 0:
                event_buffer = self.event_source.get_new_data()

                # If we haven't data then quickly check to see if there is a new
                # configuration message
                if len(event_buffer) == 0:
                    if config_listener.check_for_messages():
                        break

            if event_buffer:
                for hist in self.histograms:
                    for b in event_buffer:
                        pt = b["pulse_time"]
                        x = b["tofs"]
                        y = b["det_ids"]
                        src = b["source"] if not simulation else hist.source
                        hist.add_data(pt, x, y, src)

                if one_shot:
                    # Only plot the first histogram
                    plot_histogram(self.histograms[0])
                    # Exit the program when the graph is closed
                    return

            self.publish_histograms()
            time.sleep(0.5)

    def configure_histograms(self, config):
        """
        Configure histogramming based on the supplied configuration.

        :param config: The configuration.
        :return Tuple of the histogram sink and the histograms.
        """
        producer = Producer(config["data_brokers"])
        hist_sink = HistogramSink(producer)
        histograms = HistogramFactory.generate(config)
        return hist_sink, histograms

    def configure_event_source(self, config):
        # Check brokers and data topics exist
        if not kafka_settings_valid(config["data_brokers"], config["data_topics"]):
            raise Exception(
                "Could not configure histogramming as Kafka settings invalid"
            )

        consumer = Consumer(config["data_brokers"], config["data_topics"])
        event_source = EventSource(consumer)

        if "start" in config:
            event_source.seek_to_pulse_time(config["start"])

        return event_source

    def handle_command_message(self, message, simulation):
        if message["cmd"] == "restart":
            for hist in self.histograms:
                hist.clear_data()
        elif message["cmd"] == "config":
            try:
                if simulation:
                    logging.info("RUNNING IN SIMULATION MODE")
                    self.event_source = SimulatedEventSource1D(message)
                else:
                    self.event_source = self.configure_event_source(message)
                self.hist_sink, self.histograms = self.configure_histograms(message)
            except Exception as error:
                logging.error(f"Could not use received configuration: {error}")
        else:
            logging.warning(f'Unknown command received: {message["cmd"]}')

    def publish_histograms(self):
        # Publish histogram data
        for h in self.histograms:
            self.hist_sink.send_histogram(h.topic, h)


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
        help="configure an initial histogram from a JSON file",
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

    Main(
        args.brokers,
        args.topic,
        args.one_shot_plot,
        args.simulation_mode,
        init_hist_json,
    )
