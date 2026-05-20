import mock
import pytest

from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.exceptions import KafkaException


class TopicMetadata:
    def __init__(self, partitions=None, error=None):
        self.partitions = partitions or {}
        self.error = error


class Metadata:
    def __init__(self, topics):
        self.topics = topics


class FakeConsumer:
    def __init__(self, config, available_topics, topic_errors=None):
        self.config = config
        self.available_topics = available_topics
        self.topic_errors = topic_errors or {}
        self.queries = []
        self.closed = False

    def list_topics(self, topic=None, timeout=None):
        self.queries.append((topic, timeout))
        if topic in self.topic_errors:
            return Metadata({topic: TopicMetadata(error=self.topic_errors[topic])})
        if topic in self.available_topics:
            return Metadata({topic: TopicMetadata()})
        return Metadata({})

    def close(self):
        self.closed = True


class FakeKafkaConsumer:
    def __init__(self, config):
        self.config = config
        self.queries = []
        self.assigned_partitions = []

    def list_topics(self, topic=None, timeout=None):
        self.queries.append((topic, timeout))
        return Metadata({topic: TopicMetadata(partitions={0: None, 1: None})})

    def get_watermark_offsets(self, topic_partition, cached=False):
        return 0, 10

    def assign(self, topic_partitions):
        self.assigned_partitions = topic_partitions


class TestKafkaSettingsValidation:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumers = []
        self.available_topics = {"topic1", "topic2"}
        self.topic_errors = {}

    def create_consumer(self, config):
        consumer = FakeConsumer(
            config, self.available_topics, topic_errors=self.topic_errors
        )
        self.consumers.append(consumer)
        return consumer

    def test_checking_for_valid_topics_uses_topic_specific_metadata(self):
        with mock.patch(
            "just_bin_it.endpoints.kafka_tools.Consumer", self.create_consumer
        ):
            valid = are_kafka_settings_valid(
                ["broker1:9092", "broker2:9092"],
                ["topic1", "topic2"],
                {"security.protocol": "SASL_SSL"},
            )

        assert valid
        assert len(self.consumers) == 1
        assert self.consumers[0].config["bootstrap.servers"] == (
            "broker1:9092,broker2:9092"
        )
        assert self.consumers[0].config["security.protocol"] == "SASL_SSL"
        assert self.consumers[0].config["allow.auto.create.topics"] is False
        assert self.consumers[0].queries == [("topic1", 10), ("topic2", 10)]
        assert self.consumers[0].closed

    def test_checking_for_missing_topic_is_not_valid(self):
        self.available_topics = {"topic1"}

        with mock.patch(
            "just_bin_it.endpoints.kafka_tools.Consumer", self.create_consumer
        ):
            valid = are_kafka_settings_valid(["broker1:9092"], ["topic2"], {})

        assert not valid
        assert self.consumers[0].closed

    def test_checking_for_topic_metadata_error_is_not_valid(self):
        self.topic_errors = {"topic1": "::error::"}

        with mock.patch(
            "just_bin_it.endpoints.kafka_tools.Consumer", self.create_consumer
        ):
            valid = are_kafka_settings_valid(["broker1:9092"], ["topic1"], {})

        assert not valid
        assert self.consumers[0].closed


class TestKafkaConsumerMetadata:
    def test_assigning_topic_uses_topic_specific_metadata(self):
        kafka_consumer = FakeKafkaConsumer({})

        with mock.patch(
            "just_bin_it.endpoints.kafka_consumer.KafkaConsumer",
            mock.Mock(return_value=kafka_consumer),
        ):
            consumer = Consumer(["broker1:9092"], ["topic1"], {})

        assert kafka_consumer.queries == [("topic1", None)]
        assert [tp.partition for tp in consumer.topic_partitions] == [0, 1]
        assert [tp.offset for tp in consumer.topic_partitions] == [10, 10]
        assert kafka_consumer.assigned_partitions == consumer.topic_partitions

    def test_assigning_missing_topic_raises(self):
        kafka_consumer = FakeKafkaConsumer({})
        kafka_consumer.list_topics = mock.Mock(return_value=Metadata({}))

        with mock.patch(
            "just_bin_it.endpoints.kafka_consumer.KafkaConsumer",
            mock.Mock(return_value=kafka_consumer),
        ):
            with pytest.raises(KafkaException):
                Consumer(["broker1:9092"], ["topic1"], {})
