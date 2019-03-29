from kafka import KafkaConsumer, TopicPartition
from kafka import KafkaProducer
import time
import json
from endpoints.serialisation import deserialise_hs00
from epics import PV


# Edit these settings as appropriate
POSITIONS = [0, 10, 20, 30, 40, 50]
MOTOR_PV = "IOC:m1"
KAFKA_ADDRESS = ["localhost:9092"]
JUST_BIN_IT_COMMAND_TOPIC = "HistCommands"
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
    pv.put(position, wait=True)


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

    for i in POSITIONS:
        # Move motor to position
        print(f"moving to {i}...")
        move_motor(MOTOR_PV, i)
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

    print(histogram)
