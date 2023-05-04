# Convert command-line args to Kafka library options.
import logging


def get_kafka_security_config(
    protocol=None,
    mechanism=None,
    username=None,
    password=None,
    cafile=None,
):
    """
    Create security configuration for kafka-python from just-bin-it options.

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

    if protocol not in supported_security_protocols:
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
