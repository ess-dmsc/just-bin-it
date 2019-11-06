import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import serialise_ev42
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.utilities.fake_data_generation import generate_fake_data


TOF_RANGE = (0, 100_000_000)
DET_RANGE = (1, 512)


def generate_data(source, message_id, num_points):
    tofs, dets = generate_fake_data(TOF_RANGE, DET_RANGE, num_points)

    time_stamp = time.time_ns()

    data = serialise_ev42(source, message_id, time_stamp, tofs, dets)
    return time_stamp, data


def main(brokers, topic, num_msgs, num_points):
    producer = Producer(brokers)
    count = 0
    message_id = 1
    start_time = None
    end_time = None

    while count < num_msgs:
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

    required_args.add_argument(
        "-n",
        "--num_messages",
        type=int,
        help="the number of messages to write",
        required=True,
    )

    parser.add_argument(
        "-ne",
        "--num_events",
        type=int,
        default=1000,
        help="the number of events per message",
    )

    args = parser.parse_args()

    main(args.brokers, args.topic, args.num_messages, args.num_events)
