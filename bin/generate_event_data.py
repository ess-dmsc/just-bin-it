import os
import sys
import time

import configargparse as argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.serialisation import serialise_ev42
from just_bin_it.utilities import time_in_ns
from just_bin_it.utilities.fake_data_generation import generate_fake_data
from just_bin_it.utilities.sasl_utils import (
    add_sasl_commandline_options,
    generate_kafka_security_config,
)

TOF_RANGE = (0, 100_000_000)
DET_RANGE = (1, 10000)
DET_WIDTH = 100
DET_HEIGHT = 100


def generate_data(source, message_id, num_points):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_points)

    time_stamp = time_in_ns()

    data = serialise_ev42(source, message_id, time_stamp, tofs, dets)
    return time_stamp, data


def generate_dethist_data(source, message_id, num_points):
    dets = []

    for h in range(DET_HEIGHT):
        _, new_dets = generate_fake_data(TOF_RANGE, (0, DET_WIDTH), num_points)
        for det in new_dets:
            dets.append(h * DET_WIDTH + det)

    time_stamp = time_in_ns()

    data = serialise_ev42(source, message_id, time_stamp, [], dets)
    return time_stamp, data


def main(brokers, topic, num_msgs, num_points, kafka_security_config, det_hist=False):
    producer = Producer(brokers, kafka_security_config)
    count = 0
    message_id = 1
    start_time = None
    end_time = None

    while num_msgs == 0 or count < num_msgs:
        if det_hist:
            timestamp, data = generate_dethist_data(
                "just-bin-it", message_id, num_points
            )
        else:
            timestamp, data = generate_data("just-bin-it", message_id, num_points)
        producer.publish_message(topic, data)
        message_id += 1
        count += 1

        if not start_time:
            start_time = timestamp
        end_time = timestamp

        time.sleep(1)

    print(f"Num messages = {num_msgs}, total events = {num_msgs * num_points}")
    print(f"Start timestamp = {start_time}, end_timestamp = {end_time}")


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
        "-t", "--topic", type=str, help="the topic to write to", required=True
    )

    parser.add_argument(
        "-n",
        "--num_messages",
        type=int,
        help="the number of messages to write (0 = run continuously)",
        default=0,
    )

    parser.add_argument(
        "-ne",
        "--num_events",
        type=int,
        default=1000,
        help="the number of events per message",
    )

    parser.add_argument(
        "-dh", "--det-hist", action="store_true", help="output the data as a det hist"
    )

    parser.add_argument(
        "-c",
        "--config-file",
        is_config_file=True,
        help="configuration file",
    )

    add_sasl_commandline_options(parser)

    args = parser.parse_args()

    kafka_config = generate_kafka_security_config(
        args.security_protocol,
        args.sasl_mechanism,
        args.sasl_username,
        args.sasl_password,
        args.ssl_cafile,
    )

    main(
        args.brokers,
        args.topic,
        args.num_messages,
        args.num_events,
        kafka_config,
        args.det_hist,
    )
