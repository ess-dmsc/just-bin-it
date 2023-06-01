def add_sasl_commandline_options(parser):
    kafka_sec_args = parser.add_argument_group("Kafka security arguments")

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
