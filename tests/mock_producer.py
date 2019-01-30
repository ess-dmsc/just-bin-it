from kafka.errors import KafkaError


class MockProducer:
    def __init__(self):
        self.messages = []

    def publish_message(self, topic, message):
        self.messages.append((topic, message))


class MockThrowsProducer:
    def publish_message(self, topic, message):
        raise KafkaError("Kafka error")
