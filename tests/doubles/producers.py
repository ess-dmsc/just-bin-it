from just_bin_it.exceptions import KafkaException


class SpyProducer:
    def __init__(self, brokers=None):
        self.messages = []

    def publish_message(self, topic, message):
        self.messages.append((topic, message))


class StubProducerThatThrows:
    def publish_message(self, topic, message):
        raise KafkaException("Some Kafka error")
