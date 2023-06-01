import logging


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


def generate_kafka_security_config(
    protocol=None,
    mechanism=None,
    username=None,
    password=None,
    cafile=None,
):
    """
    Create security configuration for kafka-python from just-bin-it options.

    If no protocol is passed, PLAINTEXT is returned in the configuration.

    :param protocol: Protocol used to communicate with brokers.
    :param mechanism: SASL mechanism.
    :param username: SASL username.
    :param password: SASL password.
    :param cafile: Path to SSL CA file.
    :return: Configuration dict.
    """
    supported_security_protocols = ["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"]
    supported_sasl_mechanisms = ["PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"]

    config = {}

    if protocol is None:
        protocol = "PLAINTEXT"
    elif protocol not in supported_security_protocols:
        raise Exception(
            f"Kafka security protocol {protocol} not supported, use {supported_security_protocols}"
        )

    logging.info(f"Using Kafka security protocol {protocol}")
    config["security_protocol"] = protocol

    if "SASL_" in protocol:
        if mechanism not in supported_sasl_mechanisms:
            raise Exception(
                f"SASL mechanism {mechanism} not supported, use {supported_sasl_mechanisms}"
            )

        logging.info(f"Using SASL mechanism {mechanism}")
        config["sasl_mechanism"] = mechanism

        if not username or not password:
            raise Exception(f"Username and password are required with {protocol}")

        config["sasl_plain_username"] = username
        config["sasl_plain_password"] = password

    if "_SSL" in protocol:
        logging.info(f"Using CA certificate file {cafile}")
        config["ssl_cafile"] = cafile

    return config
