from typing import List
from endpoints.kafka_consumer import Consumer


class MockConsumerRecord:
    def __init__(self, value):
        self.value = value


class MockConsumer(Consumer):
    def __init__(self, brokers: List[str], topics: List[str]):
        super().__init__(brokers, topics)
        self.messages = []
        # Keeps track of how far through the message queue we are
        self.offset = 0

    def add_messages(self, messages):
        self.messages.extend(messages)

    def _create_consumer(self, brokers):
        return {"brokers": brokers}

    def _create_topics(self, topics):
        self.topic_partitions = topics

    def _get_new_messages(self):
        # From Kafka we get a dictionary of topics which contains a list of
        # consumer records which we want 'value' from.
        # Recreate the structure here to match that.
        data = {}

        for t in self.topic_partitions:
            records = []
            while self.offset < len(self.messages):
                msg = self.messages[self.offset]
                records.append(MockConsumerRecord(msg))
                self.offset += 1
            data[t] = records
        return data

    def _seek_by_time(self, start_time):
        # Stick it in the middle
        self.offset = len(self.messages) // 2
        return self.offset

    def _seek_by_offset(self, offset):
        self.offset = offset

    def _get_offset_range(self):
        return 0, len(self.messages) - 1


def get_fake_event_messages(num_messages):
    messages = []
    pulse_time = 0
    # The real gap would be 1/14 but we use 1/20 to make things easier.
    pulse_gap = 50_000_000  # 1/20 * 10**9

    for i in range(num_messages):
        tofs = []
        for j in range(10):
            tofs.append(j * 1_000_000)

        messages.append({"pulse_time": pulse_time, "tofs": tofs})
        pulse_time += pulse_gap
    return messages
