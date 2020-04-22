from multiprocessing import Queue
import pytest
from just_bin_it.histograms.histogram_process import StopTimeStatus, Processor
import time


VALID_CONFIG = {
    "data_brokers": ["localhost:9092", "someserver:9092"],
    "data_topics": ["my_topic"],
    "type": "hist1d",
    "tof_range": [20, 2000],
    "num_bins": 50,
    "topic": "topic0",
    "source": "source1",
}


class MockHistogrammer:
    def __init__(self):
        self.cleared = False
        self.histogramming_stopped = False
        self.times_publish_called = 0
        self.data_received = []

    def clear_histograms(self):
        self.cleared = True

    def check_stop_time_exceeded(self, timestamp: int):
        return self.histogramming_stopped

    def publish_histograms(self, timestamp=0):
        self.times_publish_called += 1

    def set_finished(self):
        self.histogramming_stopped = True

    def get_histogram_stats(self):
        return {
            "cleared": self.cleared,
            "stopped": self.histogramming_stopped,
            "times_published": self.times_publish_called,
        }

    def add_data(self, event_buffer):
        self.data_received.append(event_buffer)


class MockEventSource:
    def __init__(self):
        self.stop_time = StopTimeStatus.NOT_EXCEEDED
        self.data = []

    def get_new_data(self):
        return self.data

    def seek_to_start_time(self):
        pass

    def stop_time_exceeded(self):
        return self.stop_time


class TestHistogramProcessLowLevel:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.histogrammer = MockHistogrammer()
        self.event_source = MockEventSource()
        self.msg_queue = Queue()
        self.stats_queue = Queue()
        self.processor = Processor(
            self.histogrammer,
            self.event_source,
            self.msg_queue,
            self.stats_queue,
            publish_interval=500,
        )

    def _queue_command_message(self, message):
        self.msg_queue.put(message)
        time.sleep(0.1)

    def _get_number_of_stats_messages(self):
        time.sleep(0.1)
        # qsize is not implemented on Mac OSX, so we need to count the messages manually.
        count = 0
        while not self.stats_queue.empty():
            count += 1
            self.stats_queue.get(block=True)
        return count

    def test_unrecognised_command_does_not_trigger_stop(self):
        self._queue_command_message("unknown command")
        self.processor.run_processing()

        assert not self.processor.processing_finished
        assert not self.histogrammer.histogramming_stopped

    def test_on_clear_command_histograms_are_cleared_and_stop_not_requested(self):
        self._queue_command_message("clear")
        self.processor.run_processing()

        assert not self.processor.processing_finished
        assert not self.histogrammer.histogramming_stopped
        assert self.histogrammer.cleared

    def test_processing_requests_stop_if_stop_sent_immediately(self):
        self._queue_command_message("stop")
        self.processor.run_processing()

        assert self.processor.processing_finished
        assert self.histogrammer.histogramming_stopped

    def test_histograms_published_on_initialisation(self):
        assert self.histogrammer.times_publish_called == 1

    def test_stats_published_on_initialisation(self):
        assert self._get_number_of_stats_messages() == 1

    def test_stats_published_when_process_stopped(self):
        self._queue_command_message("stop")
        self.processor.run_processing()

        # Once on initialisation and once when processing finished
        assert self._get_number_of_stats_messages() == 2

    def test_histograms_published_when_process_stopped(self):
        self._queue_command_message("stop")
        self.processor.run_processing()

        # Once on initialisation and once when processing finished
        assert self.histogrammer.times_publish_called == 2

    def test_stats_published_when_time_to_publish_is_exceeded(self):
        # Set the publish interval to 1 ns, so it publishes
        # every time it is processed.
        self.processor.publish_interval = 1
        times_processed = 3

        for _ in range(times_processed):
            self.processor.run_processing()

        # Once on initialisation and once per time run
        assert self._get_number_of_stats_messages() == times_processed + 1

    def test_histograms_published_when_time_to_publish_is_exceeded(self):
        # Set the publish interval to 1 ns, so it publishes
        # every time it is processed.
        self.processor.publish_interval = 1
        times_processed = 3

        for _ in range(times_processed):
            self.processor.run_processing()

        # Once on initialisation and once per time run
        assert self.histogrammer.times_publish_called == times_processed + 1

    def test_processing_requests_stop_if_event_source_says_time_exceeded(self):
        self.processor.event_source.stop_time = StopTimeStatus.EXCEEDED

        self.processor.run_processing()

        assert self.processor.processing_finished
        assert self.histogrammer.histogramming_stopped

    def test_processing_does_not_request_stop_if_event_source_does_not_know_and_histogrammer_does_not_says_time_exceeded(
        self
    ):
        self.event_source.stop_time = StopTimeStatus.UNKNOWN
        self.histogrammer.histogramming_stopped = False

        self.processor.run_processing()

        assert not self.processor.processing_finished
        assert not self.histogrammer.histogramming_stopped

    def test_processing_requests_stop_if_event_source_does_not_know_and_histogrammer_says_time_exceeded(
        self
    ):
        self.event_source.stop_time = StopTimeStatus.UNKNOWN
        self.histogrammer.histogramming_stopped = True

        self.processor.run_processing()

        assert self.processor.processing_finished
        assert self.histogrammer.histogramming_stopped

    def test_processing_does_not_request_stop_if_histogrammer_says_time_exceeded_but_event_source_does_not(
        self
    ):
        self.event_source.stop_time = StopTimeStatus.NOT_EXCEEDED
        self.histogrammer.histogramming_stopped = True

        self.processor.run_processing()

        assert not self.processor.processing_finished

    def test_processing_requests_does_not_flag_stopped_if_no_reason_to_stop(self):
        self.event_source.stop_time = StopTimeStatus.NOT_EXCEEDED

        self.processor.run_processing()

        assert not self.processor.processing_finished
        assert not self.histogrammer.histogramming_stopped

    def test_if_event_data_present_then_it_is_histogrammed(self):
        self.event_source.data = [x for x in range(10)]

        self.processor.run_processing()

        assert self.histogrammer.data_received


def _create_mocked_histogram_process(monkeypatch, publish_interval=1):
    import just_bin_it.histograms.histogram_process as jbi

    def mock_create_histogrammer(configuration, start, stop):
        return MockHistogrammer()

    def mock_create_event_source(configuration, start, stop):
        return MockEventSource()

    monkeypatch.setattr(jbi, "create_histogrammer", mock_create_histogrammer)
    monkeypatch.setattr(jbi, "create_event_source", mock_create_event_source)

    process = jbi.HistogramProcess(VALID_CONFIG, None, None, publish_interval)
    return process


def test_if_stats_message_waiting_then_can_be_retrieved(monkeypatch):
    process = _create_mocked_histogram_process(monkeypatch)

    # Give initial stats message time to arrive.
    time.sleep(0.1)

    assert len(process.get_stats()) > 0

    process.stop()


def test_no_stats_message_waiting_then_get_none(monkeypatch):
    process = _create_mocked_histogram_process(monkeypatch, publish_interval=10000)

    # Give initial stats message time to arrive.
    time.sleep(0.1)

    # There will be at least one message as initialisation always sends one.
    _ = process.get_stats()

    # Immediate request for another message should return  None
    assert process.get_stats() is None

    process.stop()


def test_on_clear_message_histograms_are_cleared(monkeypatch):
    process = _create_mocked_histogram_process(monkeypatch)

    # Give it time to get going.
    time.sleep(0.1)

    process.clear()
    time.sleep(0.1)

    # Hacky way to get whether the histogrammer has been cleared
    stats = process.get_stats()
    assert stats["cleared"]

    process.stop()
