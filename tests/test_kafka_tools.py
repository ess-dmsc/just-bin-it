from just_bin_it.endpoints.kafka_tools import are_brokers_present, are_topics_present
from tests.doubles.consumer import StubConsumer


def test_checking_for_non_existent_broker_returns_none():
    assert are_brokers_present(["not_present"]) is None


def test_checking_for_non_existent_topic_returns_false():
    consumer = StubConsumer(["broker"], ["topic1"])
    assert not are_topics_present(consumer, ["not_present"])


def test_checking_for_existing_topic_returns_false():
    consumer = StubConsumer(["broker"], ["topic1"])
    assert are_topics_present(consumer, ["topic1"])
