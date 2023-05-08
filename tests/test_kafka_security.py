import pytest

from just_bin_it.endpoints.kafka_security import get_kafka_security_config


class TestKafkaSecurity:
    def test_no_protocol_returns_plaintext(self):
        c = get_kafka_security_config()
        assert len(c) == 1
        assert "PLAINTEXT" in c.values()

    def test_unsupported_protocol_throws(self):
        with pytest.raises(Exception):
            get_kafka_security_config(
                "SASL_SSL",
            )

    def test_unsupported_sasl_mechanism_throws(self):
        with pytest.raises(Exception):
            get_kafka_security_config(
                "SASL_PLAINTEXT",
                "UNSUPPORTED_PROTOCOL",
                "username",
                "password",
            )

    def test_missing_sasl_username_throws(self):
        with pytest.raises(Exception):
            get_kafka_security_config(
                "SASL_PLAINTEXT",
                "PLAIN",
                None,
                "password",
            )

    def test_missing_sasl_password_throws(self):
        with pytest.raises(Exception):
            get_kafka_security_config(
                "SASL_PLAINTEXT",
                "PLAIN",
                "username",
            )

    def test_missing_sasl_usename_and_password_throws(self):
        with pytest.raises(Exception):
            get_kafka_security_config(
                "SASL_PLAINTEXT",
                "PLAIN",
            )
