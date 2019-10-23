import argparse
import os
import sys
from kafka import KafkaConsumer, TopicPartition

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import deserialise_hs00


def main(brokers, topic):
    consumer = KafkaConsumer(bootstrap_servers=brokers)
    print(f"Topics = {consumer.topics()}")

    tp = TopicPartition(topic, 0)
    consumer.assign([tp])

    # Move to one from the end
    consumer.seek_to_end(tp)
    end = consumer.position(tp)
    consumer.seek(tp, end - 1)

    while True:
        data = []

        while not data:
            data = consumer.poll(5)

        for message in data[tp]:
            print(
                "%s %s:%d:%d: key=%s value=%s"
                % (
                    message.timestamp,
                    message.topic,
                    message.partition,
                    message.offset,
                    message.key,
                    message.value[0:20],
                )
            )
            ans = deserialise_hs00(message.value)
            print(ans)


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
        "-t",
        "--topic",
        type=str,
        help="the topic where just-bin-it is writing histogram data",
        required=True,
    )

    args = parser.parse_args()

    main(args.brokers, args.topic)
