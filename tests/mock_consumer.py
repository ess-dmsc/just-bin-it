class MockConsumerRecord:
    def __init__(self, value):
        self.value = value


class MockConsumer:
    def __init__(self, brokers, topics, messages):
        self.brokers = brokers
        self.topics = topics
        self.messages = messages

    def get_new_messages(self):
        # From Kafka we get a dictionary of topics which contains a list of
        # consumer records which we want 'value' from.
        # Recreate the structure here to match that.
        data = {}

        for t in self.topics:
            records = []
            for msg in self.messages:
                records.append(MockConsumerRecord(msg))
            data[t] = records
        return data
