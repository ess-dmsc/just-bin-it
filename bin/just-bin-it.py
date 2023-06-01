import json
import logging
import os
import sys
import time

import configargparse as argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.command_actioner import CommandActioner, ResponsePublisher
from just_bin_it.endpoints.config_listener import ConfigListener
from just_bin_it.endpoints.heartbeat_publisher import HeartbeatPublisher
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.endpoints.statistics_publisher import (
    GraphiteSender,
    StatisticsPublisher,
)
from just_bin_it.utilities import time_in_ns
from just_bin_it.utilities.sasl_utils import (
    add_sasl_commandline_options,
    generate_kafka_security_config,
)


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
        kafka_security_config,
        heartbeat_topic=None,
        stats_publisher=None,
        response_topic=None,
    ):
        """
        Constructor.

        :param config_brokers: The brokers to listen for the configuration commands on.
        :param config_topic: The topic to listen for commands on.
        :param simulation: Run in simulation mode.
        :param heartbeat_topic: The topic where to publish heartbeat messages.
        :param stats_publisher: Publisher for the histograms statistics.
        """
        self.config_topic = config_topic
        self.simulation = simulation
        self.heartbeat_topic = heartbeat_topic
        self.config_brokers = config_brokers
        self.stats_publisher = stats_publisher
        self.response_topic = response_topic
        self.kafka_security_config = kafka_security_config
        self.config_listener = None
        self.heartbeat_publisher = None
        self.hist_processes = []
        self.producer = None
        self.command_actioner = None

    def run(self):
        """
        The main loop for listening to messages and handling them.
        """
        if self.simulation:
            logging.warning("RUNNING IN SIMULATION MODE!")

        # Blocks until can connect to the config topic.
        self.create_config_listener()
        self.create_publishers()

        while True:
            # Handle configuration messages
            if self.config_listener.check_for_messages():
                msg = self.config_listener.consume_message()

                logging.warning("New command received")
                logging.warning("%s", msg)
                self.command_actioner.handle_command_message(msg, self.hist_processes)

            # Publishing of statistics and heartbeat
            curr_time_ms = time_in_ns() // 1_000_000
            if self.stats_publisher:
                self.stats_publisher.publish_histogram_stats(
                    self.hist_processes, curr_time_ms
                )

            if self.heartbeat_publisher:
                self.heartbeat_publisher.publish(curr_time_ms)

            time.sleep(0.1)

    def create_publishers(self):
        """
        Create the publishers.
        """
        self.producer = Producer(self.config_brokers, self.kafka_security_config)

        self.command_actioner = CommandActioner(
            ResponsePublisher(self.producer, self.response_topic), self.simulation
        )

        if self.heartbeat_topic:
            self.heartbeat_publisher = HeartbeatPublisher(
                self.producer, self.heartbeat_topic
            )

    def create_config_listener(self):
        """
        Create the configuration listener.

        Note: Blocks until the Kafka connection is made.
        """
        logging.info("Creating configuration consumer")
        while not are_kafka_settings_valid(
            self.config_brokers, [self.config_topic], self.kafka_security_config
        ):
            logging.error(
                "Could not connect to Kafka brokers or topic for configuration "
                "- will retry shortly"
            )
            time.sleep(5)
        self.config_listener = ConfigListener(
            Consumer(
                self.config_brokers, [self.config_topic], self.kafka_security_config
            )
        )


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
        "-rt",
        "--response-topic",
        type=str,
        help="the topic where the response messages to commands are published",
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
        default=3,
        help="sets the logging level: debug=1, info=2, warning=3, error=4, critical=5.",
    )

    add_sasl_commandline_options(parser)

    args = parser.parse_args()

    statistics_publisher = None
    if args.graphite_config_file:
        graphite_config = load_json_config_file(args.graphite_config_file)
        statistics_publisher = StatisticsPublisher(
            GraphiteSender(
                graphite_config["address"],
                graphite_config["port"],
                graphite_config["prefix"],
            ),
            graphite_config["metric"],
        )

    if 1 <= args.log_level <= 5:
        logging.basicConfig(
            format="%(asctime)s - %(message)s", level=args.log_level * 10
        )
    else:
        logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

    kafka_security_config = generate_kafka_security_config(
        args.security_protocol,
        args.sasl_mechanism,
        args.sasl_username,
        args.sasl_password,
        args.ssl_cafile,
    )

    main = Main(
        args.brokers,
        args.config_topic,
        args.simulation_mode,
        kafka_security_config,
        args.hb_topic,
        statistics_publisher,
        args.response_topic,
    )
    main.run()
