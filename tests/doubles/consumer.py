from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.serialisation import EventData


class StubConsumerRecord:
    def __init__(self, timestamp, offset, value):
        self.timestamp = timestamp
        self.offset = offset
        self.value = value


class StubConsumer(Consumer):
    def __init__(self, brokers, topics, num_partitions=1):
        super().__init__(brokers, topics)
        self.topic_names = topics
        self.topic_partitions = {}
        for i in range(num_partitions):
            self.topic_partitions[i] = {"messages": [], "offset": 0}

    def add_messages(self, messages, partition=0):
        self.topic_partitions[partition]["messages"].extend(messages)

    def topics(self):
        return self.topic_names

    def _create_consumer(self, brokers):
        return {"brokers": brokers}

    def _assign_topics(self, topics):
        pass

    def _get_new_messages(self):
        # From Kafka we get a dictionary of topics which contains a list of
        # consumer records which we want 'value' from.
        # Recreate the structure here to match that.
        data = {}

        for k, v in self.topic_partitions.items():
            records = []
            while v["offset"] < len(v["messages"]):
                msg = v["messages"][v["offset"]]
                records.append(StubConsumerRecord(msg[0], v["offset"], msg[2]))
                v["offset"] += 1
            data[k] = records
        return data

    def _seek_by_offsets(self, offsets):
        for tp, offset in zip(self.topic_partitions.values(), offsets):
            tp["offset"] = offset

    def _get_offset_range(self):
        offset_ranges = []
        for tp in self.topic_partitions.values():
            if tp["messages"]:
                offset_ranges.append((0, len(tp["messages"])))
            else:
                offset_ranges.append((0, 0))

        return offset_ranges

    def _offset_for_time(self, requested_time):
        result = []
        for tp in self.topic_partitions.values():
            count = 0
            found = False
            for msg in tp["messages"]:
                if msg[0] >= requested_time:
                    result.append(count)
                    found = True
                    break
                count += 1
            # If not found append None
            if not found:
                result.append(None)
        return result

    def _get_positions(self):
        positions = []
        for tp in self.topic_partitions.values():
            positions.append(tp["offset"])
        return positions


def get_fake_event_messages(num_messages, num_partitions=1):
    messages = []
    pulse_time = 0
    # The real gap would be 1/14 but we use 1/20 to make things easier.
    pulse_gap = 50_000_000  # 1/20 * 10**9
    offset = 0

    for _ in range(num_messages // num_partitions):
        tofs = []
        dets = []
        for j in range(10):
            tofs.append(j * 1_000_000)
            dets.append(j)

            for n in range(num_partitions):
                messages.append(
                    (
                        pulse_time,
                        offset,
                        EventData("fake_source", offset, pulse_time, tofs, dets, None),
                    )
                )
                pulse_time += pulse_gap
            offset += 1
    return messages
