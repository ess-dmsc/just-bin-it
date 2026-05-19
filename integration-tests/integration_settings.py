import os


def _env_flag(name):
    return os.environ.get(name, "").lower() in {"1", "true", "yes"}


def _brokers():
    value = os.environ.get("JBI_KAFKA_BROKERS", "localhost:9092")
    return [broker.strip() for broker in value.split(",") if broker.strip()]


BROKERS = _brokers()
KAFKA_MANAGED_EXTERNALLY = _env_flag("JBI_KAFKA_MANAGED_EXTERNALLY")
