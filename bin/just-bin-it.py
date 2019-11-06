import argparse
import json
import logging
import os
import sys
import time
import numpy as np
import graphyte

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.config_listener import ConfigListener
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.endpoints.sources import EventSource, SimulatedEventSource
from just_bin_it.histograms.histogram2d import Histogram2d
from just_bin_it.histograms.histogrammer import create_histogrammer


class StatisticsPublisher:
    def __init__(self, server, port, prefix, metric):
        """
        Constructor.

        :param server: The Graphite server to send to.
        :param port: The Graphite port to send to.
        :param prefix: The data prefix in Graphite.
        :param metric: The name to serve data in Graphite as.
        """
        self.server = server
        self.port = port
        self.prefix = prefix
        self.metric = metric

        # Initialise the connection
        graphyte.init(server, port=port, prefix=prefix)

    def send_histogram_stats(self, hist_stats):
        for i, stat in enumerate(hist_stats):
            graphyte.send(
                f"{self.metric}{i}",
                stat["sum"],
                timestamp=stat["last_pulse_time"] / 10 ** 9,
            )


def plot_histogram(hist):
    """
    Plot a histogram.

    :param hist: The histogram to plot.
    """
    import matplotlib

    matplotlib.use("TkAgg")
    from matplotlib import pyplot as plt

    if isinstance(hist, Histogram2d):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        x, y = np.meshgrid(hist.x_edges, hist.y_edges)
        # Need to transpose the data for display
        ax.pcolormesh(x, y, hist.data.T)
        plt.show()
    else:
        width = 0.7 * (hist.x_edges[1] - hist.x_edges[0])
        center = (hist.x_edges[:-1] + hist.x_edges[1:]) / 2
        plt.bar(center, hist.data, align="center", width=width)
        plt.show()


def load_json_config_file(file):
    """
    Load the specified JSON configuration file.

    :param file: The file path.
    :return: The extracted data as JSON.
    """
    try:
        path = os.path.abspath(file)
        with open(path, "r") as f:
            data = f.read()
        return json.loads(data)
    except Exception as error:
        raise Exception(f"Could not load configuration file {file}") from error


class Main:
    def __init__(
        self,
        config_brokers,
        config_topic,
        one_shot,
        simulation,
        initial_config=None,
        stats_publisher=None,
    ):
        """
        Constructor.

        :param config_brokers: The brokers to listen for the configuration commands on.
        :param config_topic: The topic to listen for commands on.
        :param one_shot: Histogram some data then plot it. Does not publish results.
        :param simulation: Run in simulation mode.
        :param initial_config: A histogram configuration to start with.
        :param stats_publisher: Publisher for the histograms statistics.
        """
        self.event_source = None
        self.histogrammer = None
        self.one_shot = one_shot
        self.simulation = simulation
        self.initial_config = initial_config
        self.config_brokers = config_brokers
        self.config_topic = config_topic
        self.config_listener = None
        self.stats_publisher = stats_publisher

    def run(self):
        if self.simulation:
            logging.info("RUNNING IN SIMULATION MODE")

        # Blocks until can connect to the config topic.
        self.create_config_listener()

        while True:
            # Handle configuration messages
            if self.initial_config or self.config_listener.check_for_messages():
                if self.initial_config:
                    # If initial configuration supplied, use it only once.
                    msg = self.initial_config
                    self.initial_config = None
                else:
                    msg = self.config_listener.consume_message()

                try:
                    logging.warning("New configuration command received")
                    self.handle_command_message(msg)
                    if not self.one_shot:
                        # Publish initial empty histograms.
                        self.histogrammer.publish_histograms()
                except Exception as error:
                    logging.error("Could not handle configuration: %s", error)

            if self.event_source is None:
                # No event source means we are waiting for a configuration.
                continue

            event_buffer = []

            while len(event_buffer) == 0:
                event_buffer = self.event_source.get_new_data()

                # If no event data then check to see if there is a new
                # configuration message
                # if len(event_buffer) == 0:
                if self.config_listener.check_for_messages():
                    break

                # See if the stop time has been exceeded
                if len(event_buffer) == 0:
                    if self.histogrammer.check_stop_time_exceeded(
                        time.time_ns() // 1_000_000
                    ):
                        break

            if event_buffer:
                self.histogrammer.add_data(event_buffer)

                if self.one_shot:
                    # Only plot the first histogram
                    plot_histogram(self.histogrammer.histograms[0])
                    # Exit the program when the graph is closed
                    return

            self.histogrammer.publish_histograms(time.time_ns())

            hist_stats = self.histogrammer.get_histogram_stats()
            logging.info("%s", json.dumps(hist_stats))

            if self.stats_publisher:
                try:
                    self.stats_publisher.send_histogram_stats(hist_stats)
                except Exception as error:
                    logging.error("Could not publish statistics: %s", error)

            time.sleep(0.5)

    def create_config_listener(self):
        """
        Create the configuration listener.

        Note: Blocks until the Kafka connection is made.
        """
        logging.info("Creating configuration consumer")
        while not are_kafka_settings_valid(self.config_brokers, [self.config_topic]):
            logging.error(
                "Could not connect to Kafka brokers or topic for configuration - will retry shortly"
            )
            time.sleep(5)
        self.config_listener = ConfigListener(
            Consumer(self.config_brokers, [self.config_topic])
        )

    def configure_histograms(self, config):
        """
        Configure histogramming based on the supplied configuration.

        :param config: The configuration.
        """
        producer = Producer(config["data_brokers"])
        self.histogrammer = create_histogrammer(
            producer, config, int(time.time() * 1000)
        )
        if self.histogrammer.start:
            self.event_source.seek_to_time(self.histogrammer.start)

    def configure_event_source(self, config):
        """
        Configure the event source.

        :param config: The configuration.
        :return: The new event source.
        """
        # Check brokers and data topics exist
        if not are_kafka_settings_valid(config["data_brokers"], config["data_topics"]):
            raise Exception("Invalid event source settings")

        consumer = Consumer(config["data_brokers"], config["data_topics"])
        event_source = EventSource(consumer)

        return event_source

    def handle_command_message(self, message):
        """
        Handle the message received.

        :param message: The message.
        """

        if message["cmd"] == "restart":
            self.histogrammer.clear_histograms()
        elif message["cmd"] == "config":
            if self.simulation:
                logging.info("RUNNING IN SIMULATION MODE")
                self.event_source = SimulatedEventSource(message)
            else:
                self.event_source = self.configure_event_source(message)
            self.configure_histograms(message)
        else:
            raise Exception(f"Unknown command type '{message['cmd']}'")


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
        "-c",
        "--config-file",
        type=str,
        help="configure an initial histogram from a file",
    )

    parser.add_argument(
        "-g",
        "--graphite-config-file",
        type=str,
        help="configuration file for publishing to Graphite",
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
        help="runs the program in simulation mode. 1-D histograms only",
    )

    parser.add_argument(
        "-l",
        "--log-level",
        type=int,
        default=3,
        help="sets the logging level: debug=1, info=2, warning=3, error=4, critical=5.",
    )

    args = parser.parse_args()

    init_hist_json = None
    if args.config_file:
        init_hist_json = load_json_config_file(args.config_file)

    stats_publisher = None
    if args.graphite_config_file:
        graphite_config = load_json_config_file(args.graphite_config_file)
        stats_publisher = StatisticsPublisher(
            graphite_config["address"],
            graphite_config["port"],
            graphite_config["prefix"],
            graphite_config["metric"],
        )

    if 1 <= args.log_level <= 5:
        logging.basicConfig(
            format="%(asctime)s - %(message)s", level=args.log_level * 10
        )
    else:
        logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

    main = Main(
        args.brokers,
        args.topic,
        args.one_shot_plot,
        args.simulation_mode,
        init_hist_json,
        stats_publisher,
    )
    main.run()
