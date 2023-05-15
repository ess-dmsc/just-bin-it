import os
import sys

import configargparse as argparse
from kafka import KafkaConsumer, TopicPartition

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.kafka_security import get_kafka_security_config
from just_bin_it.endpoints.serialisation import SCHEMAS_TO_DESERIALISERS, get_schema


def main(brokers, topic, kafka_security_config):
    consumer = KafkaConsumer(bootstrap_servers=brokers, **kafka_security_config)
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
            schema = get_schema(message.value)
            if schema in SCHEMAS_TO_DESERIALISERS:
                ans = SCHEMAS_TO_DESERIALISERS[schema](message.value)
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

    kafka_sec_args = parser.add_argument_group("Kafka security arguments")

    kafka_sec_args.add_argument(
        "-kc",
        "--kafka-config-file",
        is_config_file=True,
        help="Kafka security configuration file",
    )

    kafka_sec_args.add_argument(
        "--security-protocol",
        type=str,
        help="Kafka security protocol",
    )

    kafka_sec_args.add_argument(
        "--sasl-mechanism",
        type=str,
        help="Kafka SASL mechanism",
    )

    kafka_sec_args.add_argument(
        "--sasl-username",
        type=str,
        help="Kafka SASL username",
    )

    kafka_sec_args.add_argument(
        "--sasl-password",
        type=str,
        help="Kafka SASL password",
    )

    kafka_sec_args.add_argument(
        "--ssl-cafile",
        type=str,
        help="Kafka SSL CA certificate path",
    )

    args = parser.parse_args()

    kafka_security_config = get_kafka_security_config(
        args.security_protocol,
        args.sasl_mechanism,
        args.sasl_username,
        args.sasl_password,
        args.ssl_cafile,
    )

    main(args.brokers, args.topic, kafka_security_config)
