import argparse
import os
import sys

from confluent_kafka import Consumer, TopicPartition, OFFSET_END

from just_bin_it.exceptions import KafkaException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.serialisation import SCHEMAS_TO_DESERIALISERS, get_schema


def main(brokers, topic):
    consumer = Consumer({"bootstrap.servers": ','.join(brokers), "group.id": "mygroup"})
    print(f"Topics = {consumer.list_topics().topics.keys()}")

    tp = TopicPartition(topic, 0)
    consumer.assign([tp])

    # Move to one from the end
    consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))
    end = consumer.position([tp])[0].offset
    consumer.seek(TopicPartition(tp.topic, tp.partition, end - 1))

    while True:
        msg = consumer.poll(5)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            print(
                "%s %s:%d:%d: key=%s value=%s"
                % (
                    msg.timestamp(),
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    msg.key(),
                    msg.value()[0:20],
                )
            )
            schema = get_schema(msg.value())
            if schema in SCHEMAS_TO_DESERIALISERS:
                ans = SCHEMAS_TO_DESERIALISERS[schema](msg.value())
                print(f"\nHistogram data:\n{ans}")
                print(f"Total events: {ans['data'].sum()}")


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