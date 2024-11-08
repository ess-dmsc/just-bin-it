import copy
import json
import time
from multiprocessing import Queue

import numpy as np
import pytest

from just_bin_it.endpoints.histogram_sink import HistogramSink
from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram_factory import HistogramFactory, parse_config
from just_bin_it.histograms.histogram_process import Processor
from just_bin_it.histograms.histogrammer import HISTOGRAM_STATES, Histogrammer
from tests.doubles.producers import SpyProducer

CONFIG_1D = {
    "cmd": "config",
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "data_brokers": ["broker"],
            "data_topics": ["some_topic"],
            "tof_range": [0, 50],  # Five bins of width 10
            "num_bins": 5,
            "topic": "some_topic",
            "id": "id",
        }
    ],
}

STOP_CMD = {"cmd": "stop"}


class SpyHistogrammer:
    def __init__(self):
        self.cleared = False
        self.histogramming_stopped = False
        self.times_publish_called = 0
        self.stop = None
        self.data_received = []

    def clear_histograms(self):
        self.cleared = True

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

    def histogram_info(self):
        yield from ()


class StubEventSource:
    def __init__(self):
        self.data = []

    def get_new_data(self):
        return [self.data.pop(0)] if self.data else None

    def seek_to_start_time(self):
        pass

    def append_data(self, source_name, pulse_time_ms, time_of_flight, detector_id):
        self.data.append(
            (
                (123, pulse_time_ms),  # Kafka timestamp tuple of (type, timestamp)
                123,  # Kafka offset (irrelevant for these tests)
                (source_name, pulse_time_ms * 1e6, time_of_flight, detector_id),
            )
        )


class StubTime:
    def __init__(self):
        self.curr_time_ns = 0

    def time_in_ns(self):
        return self.curr_time_ns


class TestHistogramProcess:
    @staticmethod
    def generate_histogrammer(producer, start_time, stop_time, hist_configs):
        histograms = HistogramFactory.generate(hist_configs)
        hist_sink = HistogramSink(producer, lambda x, y, z: (x, y, z))
        return Histogrammer(histograms, start_time, stop_time), hist_sink

    def generate_processor(self, hist_configs, start_time, stop_time):
        producer = SpyProducer()
        histogrammer, hist_sink = self.generate_histogrammer(
            producer, start_time, stop_time, hist_configs
        )
        event_source = StubEventSource()
        time_source = StubTime()
        msg_queue = Queue()
        processor = Processor(
            histogrammer, event_source, hist_sink, msg_queue, Queue(), 1000, time_source
        )
        return event_source, processor, producer, time_source, msg_queue

    def test_counting_for_an_interval_gets_all_data_during_interval(self):
        config = copy.deepcopy(CONFIG_1D)
        config["interval"] = 5
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, _, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        tofs = [5, 15, 25, 35, 45]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(tofs)

        for time_offset in [0, 1000, 2000, 3000, 4000, 5000, 5001]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [6, 6, 6, 6, 6])
        assert json.loads(last_msg)["sum"] == 30
        assert json.loads(last_msg)["diff"] == 30
        assert np.allclose(
            json.loads(last_msg)["rate"],
            30 / (start_time + 5000) * 1e3,
            atol=1e-7,
            rtol=0,
        )
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_number_events_histogrammed_correspond_to_start_and_stop_times(self):
        config = copy.deepcopy(CONFIG_1D)
        config["start"] = 0
        config["stop"] = 8_000
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, _, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        tofs = [5, 15, 25, 35, 45]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(tofs)

        for time_offset in [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 8001]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [9, 9, 9, 9, 9])
        assert json.loads(last_msg)["sum"] == 45
        assert json.loads(last_msg)["diff"] == 45
        # assert json.loads(last_msg)["rate"] == 45
        np.allclose(
            json.loads(last_msg)["rate"],
            45 / (start_time + 8001) * 1e3,
            atol=1e-7,
            rtol=0,
        )
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_counting_for_duration_with_no_data_exits_after_stop_time(self):
        config = copy.deepcopy(CONFIG_1D)
        config["start"] = 0
        config["stop"] = 8 * 1000
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, time_source, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["INITIALISED"]

        # Advance time past stop time + leeway
        time_source.curr_time_ns = 15 * 1_000_000_000
        processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [0, 0, 0, 0, 0])
        assert json.loads(last_msg)["sum"] == 0
        assert json.loads(last_msg)["diff"] == 0
        assert json.loads(last_msg)["rate"] == 0
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_counting_for_an_interval_with_only_one_event_message_gets_data(self):
        config = copy.deepcopy(CONFIG_1D)
        config["start"] = 0
        config["stop"] = 8_000
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, time_source, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        tofs = [5, 15, 25, 35, 45]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(tofs)

        for time_offset in [4000]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["INITIALISED"]

        # Advance time past stop time + leeway
        time_source.curr_time_ns = 15 * 1_000_000_000
        processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [1, 1, 1, 1, 1])
        assert json.loads(last_msg)["sum"] == 5
        assert json.loads(last_msg)["diff"] == 5
        assert np.allclose(
            json.loads(last_msg)["rate"],
            5 / (start_time + 4000) * 1e3,
            atol=1e-7,
            rtol=0,
        )
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_if_wallclock_has_exceeded_stop_time_but_data_has_not_then_continues(self):
        config = copy.deepcopy(CONFIG_1D)
        config["start"] = 0
        config["stop"] = 8_000
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, time_source, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        # Advance time past stop time + leeway
        time_source.curr_time_ns = 15 * 1_000_000_000

        tofs = [5, 15, 25, 35, 45]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(tofs)

        for time_offset in [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [9, 9, 9, 9, 9])
        assert json.loads(last_msg)["sum"] == 45
        assert json.loads(last_msg)["diff"] == 40  # 45 - 5
        assert np.allclose(
            json.loads(last_msg)["rate"],
            40 / (start_time + 8000) * 1e3,
            atol=1e-7,
            rtol=0,
        )
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_counting_during_an_empty_duration_after_stop_time_data_is_ignored(self):
        config = copy.deepcopy(CONFIG_1D)
        config["start"] = 0
        config["stop"] = 8_000
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, _, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        tofs = [5, 15, 25, 35, 45]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(tofs)

        for time_offset in [8001, 8002]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [0, 0, 0, 0, 0])
        assert json.loads(last_msg)["sum"] == 0
        assert json.loads(last_msg)["diff"] == 0
        assert json.loads(last_msg)["rate"] == 0
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_open_ended_counting_for_a_while_then_stop_command_triggers_finished(self):
        config = copy.deepcopy(CONFIG_1D)
        config["start"] = 0
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, _, msg_queue = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        tofs = [5, 15, 25, 35, 45]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(tofs)

        for time_offset in [0, 1000, 2000, 3000, 4000]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        msg_queue.put("stop")
        time.sleep(0.5)

        for time_offset in [5000, 6000, 7000, 8000]:
            # Inject some fake data
            event_source.append_data(
                "::source::", start_time + time_offset, tofs, irrelevant_det_ids
            )
            processor.process()

        _, (last_hist, _, last_msg) = producer.messages[~0]

        assert np.array_equal(last_hist.data, [5, 5, 5, 5, 5])
        assert json.loads(last_msg)["sum"] == 25
        assert json.loads(last_msg)["diff"] == 25
        assert np.allclose(
            json.loads(last_msg)["rate"],
            25 / (start_time + 4000) * 1e3,
            atol=1e-7,
            rtol=0,
        )
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]

    def test_finished_is_only_published_once(self):
        config = copy.deepcopy(CONFIG_1D)
        config["interval"] = 5
        start_time, stop_time, hist_configs, _, _ = parse_config(config)

        event_source, processor, producer, _, _ = self.generate_processor(
            hist_configs, start_time, stop_time
        )

        irrelevant_tofs = [
            5,
            15,
            25,
            35,
            45,
        ]  # Values correspond to the middle of the bins
        irrelevant_det_ids = [123] * len(irrelevant_tofs)

        for time_offset in [5001, 5002, 5003]:
            # Inject some fake data
            event_source.append_data(
                "::source::",
                start_time + time_offset,
                irrelevant_tofs,
                irrelevant_det_ids,
            )
            processor.process()

        _, (_, _, first_msg) = producer.messages[0]
        _, (_, _, last_msg) = producer.messages[~0]

        assert len(producer.messages) == 2
        assert json.loads(first_msg)["state"] == HISTOGRAM_STATES["INITIALISED"]
        assert json.loads(last_msg)["state"] == HISTOGRAM_STATES["FINISHED"]


@pytest.mark.slow
class TestHistogramProcessCommands:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.histogrammer = SpyHistogrammer()
        self.event_source = StubEventSource()
        self.msg_queue = Queue()
        self.stats_queue = Queue()
        self.processor = Processor(
            self.histogrammer,
            self.event_source,
            HistogramSink(SpyProducer(), lambda x, y, z: (x, y, z)),
            self.msg_queue,
            self.stats_queue,
            publish_interval=500,
        )

    def _queue_command_message(self, message):
        self.msg_queue.put(message)
        time.sleep(0.1)

    def test_unrecognised_command_does_not_trigger_stop(self):
        self._queue_command_message("unknown command")
        self.processor.process()

        assert not self.processor.processing_finished
        assert not self.histogrammer.histogramming_stopped

    def test_on_clear_command_histograms_are_cleared_and_stop_not_requested(self):
        self._queue_command_message("clear")
        self.processor.process()

        assert not self.processor.processing_finished
        assert not self.histogrammer.histogramming_stopped
        assert self.histogrammer.cleared

    def test_processing_requests_stop_if_stop_sent_immediately(self):
        self._queue_command_message("stop")
        self.processor.process()

        assert self.processor.processing_finished
        assert self.histogrammer.histogramming_stopped


@pytest.mark.slow
class TestHistogramProcessPublishing:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.histogrammer = SpyHistogrammer()
        self.event_source = StubEventSource()
        self.producer = SpyProducer()
        self.msg_queue = Queue()
        self.stats_queue = Queue()
        self.processor = Processor(
            self.histogrammer,
            self.event_source,
            HistogramSink(self.producer, lambda x, y, z: (x, y, z)),
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

    def test_published_on_initialisation(self):
        assert self._get_number_of_stats_messages() == 1

    def test_published_when_process_stopped(self):
        self._queue_command_message("stop")
        self.processor.process()

        # Once on initialisation and once when processing finished
        assert self._get_number_of_stats_messages() == 2

    def test_published_when_time_to_publish_is_exceeded(self):
        # Set the publish interval to 1 ms, so it publishes
        # every time it is processed.
        self.processor.publish_interval = 1
        times_processed = 3

        for _ in range(times_processed):
            self.processor.process()
            time.sleep(0.01)

        # Once on initialisation and once per time run
        assert self._get_number_of_stats_messages() == times_processed + 1
