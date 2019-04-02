from kafka import KafkaConsumer, TopicPartition
from kafka import KafkaProducer
import time
import json
from endpoints.serialisation import deserialise_hs00
from epics import PV


# Edit these settings as appropriate
POSITIONS_1 = [0, 1, 2, 3, 4, 5]
POSITIONS_2 = [0, 1, 2, 3, 4, 5]
MOTOR_PV_1 = "IOC:m1"
MOTOR_PV_2 = "IOC:m2"
KAFKA_ADDRESS = ["localhost:9092"]
JUST_BIN_IT_COMMAND_TOPIC = "hist_commands"
EVENT_TOPIC = "LOQ_events"
HISTOGRAM_TOPIC = "hist-topic2"
COUNT_TIME_SECS = 5

CONFIG = {
    "data_brokers": KAFKA_ADDRESS,
    "data_topics": [EVENT_TOPIC],
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100_000_000],
            "num_bins": 50,
            "topic": HISTOGRAM_TOPIC,
        }
    ],
}


def get_total_counts(consumer, topic):
    data = {}
    consumer.seek_to_end(topic)

    while len(data) == 0:
        data = consumer.poll(5)
    ans = deserialise_hs00(data[topic][-1].value)
    return sum(ans["data"])


def move_motor(motor_pv, position):
    # TODO: Using EPICS for now, but at some point we should use NICOS
    pv = PV(motor_pv)
    pv.put(position, wait=True, timeout=120)


if __name__ == "__main__":
    # Start counting
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS)
    producer.send(JUST_BIN_IT_COMMAND_TOPIC, bytes(json.dumps(CONFIG), "utf-8"))
    producer.flush()
    time.sleep(2)

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_ADDRESS)
    topic = TopicPartition(HISTOGRAM_TOPIC, 0)
    consumer.assign([topic])
    consumer.seek_to_end(topic)

    histogram = []
    last_value = 0

    for i in POSITIONS_1:
        # Move motor 1 to position
        print(f"moving 1 to {i}...")
        move_motor(MOTOR_PV_1, i)

        for j in POSITIONS_2:
            # Move motors to position
            print(f"moving 2 to {j}...")
            move_motor(MOTOR_PV_2, j)
            last_value = get_total_counts(consumer, topic)
            print("value after move =", last_value)

            # Collect data for some number of seconds
            print("counting...")
            time.sleep(COUNT_TIME_SECS)

            # Get total counts
            next_value = get_total_counts(consumer, topic)

            # Counts for "data collection" is current count minus the counts after move
            histogram.append(next_value - last_value)
            last_value = next_value
            print("value after counting =", last_value)

    row = 0
    num_rows = len(histogram) / len(POSITIONS_1)
    while row < num_rows:
        start = row * len(POSITIONS_1)
        print(histogram[start : start + len(POSITIONS_2)])
        row += 1
