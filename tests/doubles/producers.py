from just_bin_it.exceptions import KafkaException


class SpyProducer:
    def __init__(self, brokers=None):
        self.messages = []

    def publish_message(self, topic, message):
        self.messages.append((topic, message))

    def flush(self, timeout):
        self.flush_timeout = timeout
        return 0


class StubProducerThatThrows:
    def publish_message(self, topic, message):
        raise KafkaException("Some Kafka error")

    def flush(self, timeout):
        return 0
