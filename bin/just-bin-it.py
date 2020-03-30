import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.config_listener import ConfigListener
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.exceptions import KafkaException
from just_bin_it.histograms.histogram_factory import parse_config
from just_bin_it.histograms.histogram_process import HistogramProcess
from just_bin_it.utilities import time_in_ns
from just_bin_it.utilities.statistics_publisher import StatisticsPublisher


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
        simulation,
        heartbeat_topic=None,
        initial_config=None,
        stats_publisher=None,
    ):
        """
        Constructor.

        :param config_brokers: The brokers to listen for the configuration commands on.
        :param config_topic: The topic to listen for commands on.
        :param simulation: Run in simulation mode.
        :param heartbeat_topic: The topic where to publish heartbeat messages.
        :param initial_config: A histogram configuration to start with.
        :param stats_publisher: Publisher for the histograms statistics.
        """
        self.event_source = None
        self.histogrammer = None
        self.simulation = simulation
        self.initial_config = initial_config
        self.config_brokers = config_brokers
        self.config_topic = config_topic
        self.heartbeat_topic = heartbeat_topic
        self.config_listener = None
        self.stats_publisher = stats_publisher
        self.heartbeat_publisher = None
        self.hist_process = []
        self.stats_interval_ms = 1000
        self.time_to_publish_stats = 0
        self.heartbeat_interval_ms = 1000
        self.time_to_publish_heartbeat = 0

    def run(self):
        """
        The main loop for listening to messages and handling them.
        """
        if self.simulation:
            logging.warning("RUNNING IN SIMULATION MODE!")

        # Blocks until can connect to the config topic.
        self.create_config_listener()
        self.create_heartbeat_producer()

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
                    logging.warning("%s", msg)
                    self.handle_command_message(msg)
                except Exception as error:
                    logging.error("Could not handle configuration: %s", error)

            # Handle publishing of statistics
            curr_time = time_in_ns()
            if curr_time // 1_000_000 > self.time_to_publish_stats:
                self.publish_stats(curr_time)

            if curr_time // 1_000_000 > self.time_to_publish_heartbeat:
                self.publish_heartbeat(curr_time)

            time.sleep(0.1)

    def create_heartbeat_producer(self):
        """
        Create the Kafka producer for publishing heart-beat messages.
        """
        if self.heartbeat_topic:
            self.heartbeat_publisher = Producer(self.config_brokers)

    def publish_heartbeat(self, current_time):
        """
        Publish heart-beat messages.

        :param current_time: The current time.
        """
        if self.heartbeat_publisher:
            msg = {
                "message": current_time // 1_000_000,
                "message_interval": self.heartbeat_interval_ms,
            }
            try:
                self.heartbeat_publisher.publish_message(
                    self.heartbeat_topic, bytes(json.dumps(msg), "utf-8")
                )
            except KafkaException as error:
                logging.error("Could not publish heartbeat: %s", error)
            self.time_to_publish_heartbeat = (
                current_time // 1_000_000 + self.heartbeat_interval_ms
            )
            self.time_to_publish_heartbeat -= (
                self.time_to_publish_heartbeat % self.heartbeat_interval_ms
            )

    def publish_stats(self, current_time):
        """
        Publish the statistics from the histogram processes.

        :param current_time: The current time.
        """
        if self.stats_publisher:
            for i, process in enumerate(self.hist_process):
                try:
                    stats = self.hist_process[i].get_stats()
                    if stats:
                        self.stats_publisher.send_histogram_stats(stats, i)
                except Exception as error:
                    logging.error("Could not publish statistics: %s", error)
        self.time_to_publish_stats = current_time // 1_000_000 + self.stats_interval_ms
        self.time_to_publish_stats -= (
            self.time_to_publish_stats % self.stats_interval_ms
        )

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

    def stop_processes(self):
        """
        Request the processes to stop.
        """
        for process in self.hist_process:
            try:
                process.stop()
            except Exception as error:
                # Process might have killed itself already
                logging.info("Stopping process failed %s", error)
        self.hist_process.clear()

    def handle_command_message(self, message):
        """
        Handle the message received.

        :param message: The message.
        """
        if message["cmd"] == "restart":
            for process in self.hist_process:
                process.clear()
        elif message["cmd"] == "config":
            logging.info("Stopping existing processes")
            self.stop_processes()

            start, stop, hist_configs = parse_config(message)

            try:
                for config in hist_configs:
                    process = HistogramProcess(config, start, stop, self.simulation)
                    self.hist_process.append(process)
            except Exception as error:
                # If one fails then close any that were started then rethrow
                self.stop_processes()
                raise error
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
        "-t", "--config-topic", type=str, help="the configuration topic", required=True
    )

    parser.add_argument(
        "-hb", "--hb-topic", type=str, help="the topic where the heartbeat is published"
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
        "-s",
        "--simulation-mode",
        action="store_true",
        help="runs the program in simulation mode",
    )

    parser.add_argument(
        "-l",
        "--log-level",
        type=int,
        default=2,
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
        args.config_topic,
        args.simulation_mode,
        args.hb_topic,
        init_hist_json,
        stats_publisher,
    )
    main.run()
