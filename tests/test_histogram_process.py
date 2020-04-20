from multiprocessing import Queue
import pytest
from just_bin_it.exceptions import KafkaException
from just_bin_it.histograms.histogram_process import (
    HistogramProcess,
    _create_process,
    process_command_message,
    stop_time_exceeded,
    StopTimeStatus,
)


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
    msg_queue = Queue()
    stats_queue = Queue()

    p = _create_process(msg_queue, stats_queue, {}, 0, 0, False, use_mocks=True)
    p.start()

    assert p.is_alive()
    msg_queue.put("quit")

    p.join(5)

    assert not p.is_alive()


class MockHistogrammer:
    def __init__(self):
        self.cleared = False
        self.stop_time_exceeded = False

    def clear_histograms(self):
        self.cleared = True

    def check_stop_time_exceeded(self, timestamp: int):
        return self.stop_time_exceeded


def test_on_stop_command_requests_stop():
    histogrammer = MockHistogrammer()
    msg_queue = Queue()
    msg_queue.put("stop")

    assert process_command_message(msg_queue, histogrammer)


def test_unrecognised_command_ignored():
    histogrammer = MockHistogrammer()
    msg_queue = Queue()
    msg_queue.put("unrecognised")

    assert not process_command_message(msg_queue, histogrammer)


def test_on_clear_command_histograms_are_cleared_and_stop_not_requested():
    histogrammer = MockHistogrammer()
    msg_queue = Queue()
    msg_queue.put("clear")

    assert not process_command_message(msg_queue, histogrammer)
    assert histogrammer.cleared


def test_if_stop_time_not_exceeded_then_stop_time_exceeded_is_false():
    histogrammer = MockHistogrammer()

    assert not stop_time_exceeded(StopTimeStatus.NOT_EXCEEDED, histogrammer)


def test_if_stop_time_is_exceeded_then_stop_time_exceeded_is_true():
    histogrammer = MockHistogrammer()

    assert stop_time_exceeded(StopTimeStatus.EXCEEDED, histogrammer)


def test_if_stop_time_is_unknown_then_stop_time_exceeded_if_histogrammer_says_so():
    histogrammer = MockHistogrammer()
    histogrammer.stop_time_exceeded = True

    assert stop_time_exceeded(StopTimeStatus.UNKNOWN, histogrammer)


def test_if_stop_time_is_unknown_then_stop_time_not_exceeded_if_histogrammer_says_not():
    histogrammer = MockHistogrammer()
    histogrammer.stop_time_exceeded = False

    assert not stop_time_exceeded(StopTimeStatus.UNKNOWN, histogrammer)
