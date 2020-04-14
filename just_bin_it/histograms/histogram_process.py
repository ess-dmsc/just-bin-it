import json
import logging
from multiprocessing import Process, Queue
import time
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.endpoints.sources import (
    EventSource,
    SimulatedEventSource,
    StopTimeStatus,
)
from just_bin_it.exceptions import KafkaException, JustBinItException
from just_bin_it.histograms.histogrammer import Histogrammer
from just_bin_it.histograms.histogram_factory import HistogramFactory
from just_bin_it.utilities import time_in_ns
from just_bin_it.utilities.mock_consumer import MockConsumer
from just_bin_it.utilities.mock_producer import MockProducer


def create_simulated_event_source(configuration, start, stop):
    """
    Create a simulated event source.

    :param configuration The configuration.
    :param start: The start time.
    :param stop: The stop time.
    :return: The created event source.
    """
    return SimulatedEventSource(configuration, start, stop)


def create_event_source(consumer, start, stop):
    """
    Create the event source.

    :param consumer: The consumer to use.
    :param start: The start time.
    :param stop: The stop time.
    :return: The created event source.
    """
    event_source = EventSource(consumer, start, stop)

    if start:
        event_source.seek_to_start_time()
    return event_source


def create_histogrammer(producer, configuration, start, stop):
    """
    Create a histogrammer.

    :param producer: The producer.
    :param configuration: The configuration.
    :param start: The start time.
    :param stop: The stop time.
    :return: The created histogrammer.
    """
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


def _histogramming_process(
    msg_queue,
    stats_queue,
    configuration,
    start,
    stop,
    simulation=False,
    use_mocks=False,
):
    publish_interval = 500
    time_to_publish = 0
    stop_processing = False

    producer = MockProducer() if use_mocks else Producer(configuration["data_brokers"])
    histogrammer = create_histogrammer(producer, configuration, start, stop)

    consumer = MockConsumer(configuration["data_brokers"], configuration["data_topics"])
    if not use_mocks and not simulation:
        consumer = Consumer(configuration["data_brokers"], configuration["data_topics"])

    if simulation:
        event_source = create_simulated_event_source(configuration, start, stop)
    else:
        event_source = create_event_source(consumer, start, stop)

    # Publish initial empty histograms.
    histogrammer.publish_histograms()

    while not stop_processing:
        event_buffer = []

        while len(event_buffer) == 0:
            # Check message queue to see if we should stop
            if not msg_queue.empty():
                msg = msg_queue.get(True)
                if msg == "quit":
                    logging.info("Stopping histogramming process")
                    stop_processing = True
                    break
                elif msg == "clear":
                    logging.info("Clearing histograms")
                    histogrammer.clear_histograms()

            event_buffer = event_source.get_new_data()
            kafka_stop_time_exceeded = event_source.stop_time_exceeded()

            if kafka_stop_time_exceeded == StopTimeStatus.EXCEEDED:
                # According to Kafka the stop time has been exceeded.
                # There may be some data in the event buffer to add though.
                logging.info("Stop time exceeded according to Kafka")
                stop_processing = True
                break

            # See if the stop time has been exceeded by the wall-clock.
            # This may happen if there has not been any data for a while, so
            # Kafka cannot tell us if the stop time has been exceeded.
            if (
                len(event_buffer) == 0
                and kafka_stop_time_exceeded == StopTimeStatus.UNKNOWN
            ):
                if histogrammer.check_stop_time_exceeded(time_in_ns() // 1_000_000):
                    logging.info("Stop time exceeded according to wall-clock")
                    stop_processing = True
                    break

        if event_buffer:
            histogrammer.add_data(event_buffer)

        if stop_processing:
            histogrammer.set_finished()

        # Only publish at specified rate or if the process is stopping.
        curr_time = time_in_ns()
        if curr_time // 1_000_000 > time_to_publish or stop_processing:
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
    use_mocks=False,
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
    :param use_mocks: Use Kafka mocks when unit-testing.
    """
    try:
        _histogramming_process(
            msg_queue, stats_queue, configuration, start, stop, simulation, use_mocks
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
    use_mocks=False,
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
            use_mocks,
        ),
    )


class HistogramProcess:
    def __init__(self, configuration, start_time, stop_time, simulation=False):
        # Check brokers and data topics exist (skip in simulation)
        if not simulation and not are_kafka_settings_valid(
            configuration["data_brokers"], configuration["data_topics"]
        ):
            raise KafkaException("Invalid Kafka settings")

        self._msg_queue = Queue()
        self._stats_queue = Queue()
        self._process = _create_process(
            self._msg_queue,
            self._stats_queue,
            configuration,
            start_time,
            stop_time,
            simulation,
        )
        self._process.start()

    def stop(self):
        if self._process.is_alive():
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
