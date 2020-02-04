import json
import logging
from multiprocessing import Process, Queue
import time
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.endpoints.sources import EventSource, SimulatedEventSource
from just_bin_it.exceptions import KafkaException, JustBinItException
from just_bin_it.histograms.histogrammer import Histogrammer
from just_bin_it.histograms.histogram_factory import HistogramFactory


def create_simulated_event_source(configuration):
    """
    Create a simulated event source.

    :param configuration The configuration.
    :return: The created event source.
    """
    return SimulatedEventSource(configuration)


def create_event_source(consumer, start):
    """
    Create the event source.

    :param consumer: The consumer to use.
    :param start: The start time.
    :return: The created event source.
    """
    event_source = EventSource(consumer)

    if start:
        event_source.seek_to_time(start)
    return event_source


def create_histogrammer(configuration, start, stop):
    """
    Create a histogrammer.

    :param configuration: The configuration.
    :param start: The start time.
    :param stop: The stop time.
    :return: The created histogrammer.
    """
    producer = Producer(configuration["data_brokers"])
    histograms = HistogramFactory.generate([configuration])
    return Histogrammer(producer, histograms, start, stop)


def publish_data(histogrammer, current_time, stats_queue):
    """
    Publish data, both histograms and statistics

    :param histogrammer: The histogrammer.
    :param current_time: The time to associate the publishing with.
    :param stats_queue: The queue to send statistics to.
    """
    histogrammer.publish_histograms(current_time)
    hist_stats = histogrammer.get_histogram_stats()
    logging.info("%s", json.dumps(hist_stats))
    stats_queue.put(hist_stats)


def _process(
    msg_queue,
    stats_queue,
    configuration,
    start,
    stop,
    simulation=False,
    histogrammer=None,
    event_source=None,
):
    publish_interval = 500
    time_to_publish = 0
    exit_requested = False

    if histogrammer is None:
        histogrammer = create_histogrammer(configuration, start, stop)

    if event_source is None:
        if simulation:
            event_source = create_simulated_event_source(configuration)
        else:
            consumer = Consumer(
                configuration["data_brokers"], configuration["data_topics"]
            )
            event_source = create_event_source(consumer, start)

    # Publish initial empty histograms.
    histogrammer.publish_histograms()

    while not exit_requested:
        event_buffer = []

        while len(event_buffer) == 0:
            # Check message queue to see if we should stop
            if not msg_queue.empty():
                msg = msg_queue.get(True)
                if msg == "quit":
                    logging.info("Stopping histogramming process")
                    exit_requested = True
                    break
                elif msg == "clear":
                    logging.info("Clearing histograms")
                    histogrammer.clear_histograms()

            event_buffer = event_source.get_new_data()

            # See if the stop time has been exceeded
            if len(event_buffer) == 0:
                if histogrammer.check_stop_time_exceeded(time.time_ns() // 1_000_000):
                    break

        if event_buffer:
            histogrammer.add_data(event_buffer)

        # Only publish at specified rate
        curr_time = time.time_ns()
        if curr_time // 1_000_000 > time_to_publish:
            publish_data(histogrammer, curr_time, stats_queue)
            time_to_publish = curr_time // 1_000_000 + publish_interval
            time_to_publish -= time_to_publish % publish_interval

        time.sleep(0.01)


def process(
    msg_queue,
    stats_queue,
    configuration,
    start,
    stop,
    simulation=False,
    histogrammer=None,
    event_source=None,
):
    """
    The target to run in a multi-processing instance for histogramming.

    Note: passing objects into a process requires the classes to be pickleable.
    The histogrammer and event source classes are not pickleable, so need to be created
    within the process

    :param msg_queue: The message queue for communicating with the process.
    :param stats_queue: The queue to send statistics to.
    :param configuration: The histogramming configuration.
    :param start: The start time.
    :param stop: The stop time.
    :param simulation: Whether to run in simulation.
    :param histogrammer: The histogrammer to use - unit tests only.
    :param event_source: The event-source to use - unit tests only.
    """
    try:
        _process(
            msg_queue,
            stats_queue,
            configuration,
            start,
            stop,
            simulation,
            histogrammer,
            event_source,
        )
    except JustBinItException as error:
        logging.error("Histogram process failed: {}", error)


def _create_process(
    msg_queue,
    stats_queue,
    configuration,
    start,
    stop,
    simulation=False,
    histogrammer=None,
    event_source=None,
):
    return Process(
        target=process,
        args=(
            msg_queue,
            stats_queue,
            configuration,
            start,
            stop,
            simulation,
            histogrammer,
            event_source,
        ),
    )


class HistogramProcess:
    def __init__(self, configuration, start, stop, simulation=False):
        # Check brokers and data topics exist (skip in simulation)
        if not simulation and not are_kafka_settings_valid(
            configuration["data_brokers"], configuration["data_topics"]
        ):
            raise KafkaException("Invalid Kafka settings")

        self._msg_queue = Queue()
        self._stats_queue = Queue()
        self._process = _create_process(
            self._msg_queue, self._stats_queue, configuration, start, stop, simulation
        )
        self._process.start()

    def stop(self):
        self._msg_queue.put("quit")
        self._process.join()

    def clear(self):
        self._msg_queue.put("clear")
        self._process.join()

    def get_stats(self):
        # Empty the queue and only return the most recent value
        most_recent = None
        while not self._stats_queue.empty():
            most_recent = self._stats_queue.get(False)
        return most_recent
