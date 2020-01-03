from multiprocessing import Process, Queue
import pytest
from just_bin_it.histograms.histogram_process import HistogramProcess, process
from just_bin_it.exceptions import KafkaException
from just_bin_it.histograms.histogrammer import create_histogrammer
from just_bin_it.endpoints.sources import EventSource
from tests.mock_consumer import MockConsumer
from tests.mock_producer import MockProducer


VALID_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "type": "hist1d",
    "tof_range": [20, 2000],
    "num_bins": 50,
    "topic": "topic0",
    "source": "source1",
}

INVALID_KAFKA_CONFIG = {
    "data_brokers": ["cannot_exist:9092"],
    "data_topics": ["my_topic"],
    "type": "hist1d",
    "tof_range": [20, 2000],
    "num_bins": 50,
    "topic": "topic0",
    "source": "source1",
}


def test_histogram_process_throws_if_cannot_connect_to_kafka():
    with pytest.raises(KafkaException):
        HistogramProcess(INVALID_KAFKA_CONFIG, 0, 100)


def test_process_exits_when_requested():
    histogrammer = create_histogrammer(MockProducer(), VALID_CONFIG)
    consumer = MockConsumer(["broker"], ["topic"])
    event_source = EventSource(consumer, lambda x: x)
    queue = Queue()
    queue.put("quit")

    p = Process(target=process, args=(queue, histogrammer, event_source))
    p.start()

    assert p.is_alive()

    p.join(5)

    assert not p.is_alive()
