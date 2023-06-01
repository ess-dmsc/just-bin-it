import json
import logging
import time
from multiprocessing import Process, Queue

from just_bin_it.endpoints.histogram_sink import HistogramSink
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.serialisation import (
    SCHEMAS_TO_DESERIALISERS,
    SCHEMAS_TO_SERIALISERS,
)
from just_bin_it.endpoints.sources import (
    EventSource,
    SimulatedEventSource,
)
from just_bin_it.histograms.histogram_factory import HistogramFactory
from just_bin_it.histograms.histogrammer import Histogrammer
from just_bin_it.utilities import time_in_ns


def create_simulated_event_source(configuration, start, stop):
    """
    Create a simulated event source.

    :param configuration: The configuration.
    :param start: The start time.
    :param stop: The stop time.
    :return: The created event source.
    """
    return SimulatedEventSource(configuration, start, stop)


def create_event_source(configuration, start, stop, deserialise_func):
    """
    Create an event source.

    :param configuration: The configuration.
    :param start: The start time.
    :param stop: The stop time.
    :param deserilise_func: The FlatBuffers deserialisation function.
    :return: The created event source.
    """
    consumer = Consumer(configuration["data_brokers"], configuration["data_topics"])
    time.sleep(5)
    event_source = EventSource(consumer, start, stop, deserialise_func)

    if start:
        event_source.seek_to_start_time()
    return event_source


def create_histogrammer(configuration, start, stop, hist_schema):
    """
    Create a histogrammer.

    :param configuration: The configuration.
    :param start: The start time.
    :param stop: The stop time.
    :param hist_schema: The output schema.
    :return: The created histogrammer.
    """
    producer = Producer(configuration["data_brokers"])
    hist_sink = HistogramSink(producer, SCHEMAS_TO_SERIALISERS[hist_schema])
    histograms = HistogramFactory.generate([configuration])
    return Histogrammer(hist_sink, histograms, start, stop)


class Time:
    def time_in_ns(self):
        return time_in_ns()


class Processor:
    def __init__(
        self, histogrammer, event_source, msg_queue, stats_queue, publish_interval, time_source=Time()
    ):
        """
        Constructor.

        :param histogrammer: The histogrammer for this process.
        :param event_source: The event source for this process.
        :param msg_queue: The queue for receiving messages from outside the process
        :param stats_queue: The queue for publishing stats to the "outside".
        :param publish_interval: How often to publish histograms and stats in milliseconds.
        """
        assert publish_interval > 0

        self.time_to_publish = 0
        self.histogrammer = histogrammer
        self.event_source = event_source
        self.msg_queue = msg_queue
        self.stats_queue = stats_queue
        self.publish_interval = publish_interval
        self.processing_finished = False
        self.time = time_source

        # Publish initial empty histograms and stats.
        self.publish_data(self.time.time_in_ns())

    def run_processing(self):
        """
        Run the processing chain once.
        """
        if not self.msg_queue.empty():
            self.processing_finished |= self.process_command_message()

        if not self.processing_finished:
            event_buffer = self.event_source.get_new_data()

            if event_buffer:
                # Even if the stop time has been exceeded there still may be data
                # in the buffer to add.
                self.histogrammer.add_data(event_buffer)

                if self.histogrammer.is_finished():
                    self.processing_finished = True
            else:
                self.processing_finished |= self.is_stop_time_exceeded(
                    self.time.time_in_ns() // 1_000_000,
                    self.histogrammer.stop
                )

        if self.processing_finished:
            self.histogrammer.set_finished()

        # Only publish at specified rate or if the process is stopping.
        curr_time = self.time.time_in_ns()
        if curr_time // 1_000_000 > self.time_to_publish or self.processing_finished:
            self.publish_data(curr_time)
            self.time_to_publish = curr_time // 1_000_000 + self.publish_interval
            self.time_to_publish -= self.time_to_publish % self.publish_interval

    def is_stop_time_exceeded(self, current_time_ms, stop_time_ms, stop_leeway_ms=5000):
        if stop_time_ms is None:
            return False

        if current_time_ms > stop_time_ms + stop_leeway_ms:
            return True
        return False

    def process_command_message(self):
        """
        Processes any messages received from outside.

        :return: True, if a stop has been requested.
        """
        msg = self.msg_queue.get(block=True, timeout=0.05)
        if msg == "stop":
            logging.info("Stopping histogramming process")
            return True
        elif msg == "clear":
            logging.info("Clearing histograms")
            self.histogrammer.clear_histograms()
        return False

    def publish_data(self, current_time):
        """
        Publish data, both histograms and statistics.

        :param current_time: The time to associate the publishing with.
        """
        self.histogrammer.publish_histograms(current_time)
        hist_stats = self.histogrammer.get_histogram_stats()
        logging.info("%s", json.dumps(hist_stats))
        self.stats_queue.put(hist_stats)


def run_processing(
    msg_queue,
    stats_queue,
    configuration,
    start,
    stop,
    hist_schema,
    event_schema,
    publish_interval,
    simulation=False,
):
    """
    The target to run in a multi-processing instance for histogramming.

    Note: passing objects into a process requires the classes to be pickleable.
    The histogrammer and event source classes are not pickleable, so need to be
    created within the process.

    In effect, this is the 'main' for a process so all the dependencies etc. are
    set up here.

    :param msg_queue: The message queue for communicating with the process.
    :param stats_queue: The queue to send statistics to.
    :param configuration: The histogramming configuration.
    :param start: The start time.
    :param stop: The stop time.
    :param hist_schema: The schema to use for writing histograms.
    :param event_schema: The schema of the event messages.
    :param publish_interval: How often to publish histograms and stats in milliseconds.
    :param simulation: Whether to run in simulation.
    """
    histogrammer = None
    try:
        # Setting up
        histogrammer = create_histogrammer(configuration, start, stop, hist_schema)

        if simulation:
            event_source = create_simulated_event_source(configuration, start, stop)
        else:
            deserialise_func = SCHEMAS_TO_DESERIALISERS[event_schema]
            event_source = create_event_source(
                configuration, start, stop, deserialise_func
            )

        processor = Processor(
            histogrammer, event_source, msg_queue, stats_queue, publish_interval
        )

        # Start up the processing
        while not processor.processing_finished:
            processor.run_processing()
            time.sleep(0.01)
    except Exception as error:
        logging.error("Histogram process failed: %s", error)
        if histogrammer:
            try:
                # Try to send failure message
                histogrammer.send_failure_message(time_in_ns(), str(error))
            except Exception as send_error:
                logging.error("Could not send failure message: %s", send_error)


class HistogramProcess:
    def __init__(
        self,
        configuration,
        start_time,
        stop_time,
        hist_schema,
        event_schema,
        publish_interval=500,
        simulation=False,
    ):
        """
        Constructor.

        :param configuration: The histogramming configuration.
        :param start_time: The start time.
        :param stop_time: The stop time.
        :param hist_schema: the output schema to use.
        :param publish_interval: How often to publish histograms and stats in milliseconds.
        :param simulation: Whether to run in simulation.
        """
        self._msg_queue = Queue()
        self._stats_queue = Queue()
        self._process = Process(
            target=run_processing,
            args=(
                self._msg_queue,
                self._stats_queue,
                configuration,
                start_time,
                stop_time,
                hist_schema,
                event_schema,
                publish_interval,
                simulation,
            ),
        )

    def start(self):
        self._process.start()

    def stop(self):
        if self._process.is_alive():
            self._msg_queue.put("stop")
            # Must empty the stats queue otherwise it could potentially stop
            # the process from closing.
            while not self._stats_queue.empty():
                self._stats_queue.get()

            self._process.join()

    def clear(self):
        if self._process.is_alive():
            self._msg_queue.put("clear")

    def get_stats(self):
        # Empty the queue and only return the most recent value
        most_recent = None
        while not self._stats_queue.empty():
            most_recent = self._stats_queue.get(False)
        return most_recent
