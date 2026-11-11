from unittest import mock

from just_bin_it.endpoints import kafka_consumer
from just_bin_it.endpoints.kafka_consumer import Consumer


class TestConsumerOptions:
    def test_consumer_does_not_commit_offsets(self):
        with mock.patch.object(kafka_consumer, "KafkaConsumer") as factory:
            factory.return_value.list_topics.return_value.topics = {}
            try:
                Consumer(["broker"], ["topic"], {})
            except Exception:
                pass

        options = factory.call_args[0][0]
        assert options["enable.auto.commit"] is False
