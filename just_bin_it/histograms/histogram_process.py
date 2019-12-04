import json
import logging
from multiprocessing import Process, Queue
import time
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.kafka_producer import Producer
from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.endpoints.sources import EventSource, SimulatedEventSource
from just_bin_it.histograms.histogrammer import Histogrammer
from just_bin_it.histograms.histogram_factory import HistogramFactory


def configure_event_source(config):
    """
    Configure the event source.

    :param config: The configuration.
    :return: The new event source.
    """
    # Check brokers and data topics exist
    if not are_kafka_settings_valid(config["data_brokers"], config["data_topics"]):
        raise Exception("Invalid event source settings")

    consumer = Consumer(config["data_brokers"], config["data_topics"])
    event_source = EventSource(consumer)

    return event_source


def process(msg_queue, configuration, start, stop, simulation=False):
    # Set up
    if simulation:
        event_source = SimulatedEventSource(configuration)
    else:
        event_source = configure_event_source(configuration)
    producer = Producer(configuration["data_brokers"])
    histograms = HistogramFactory.generate([configuration])
    histogrammer = Histogrammer(producer, histograms, start, stop)

    if start:
        event_source.seek_to_time(histogrammer.start)

    # Publish initial empty histograms.
    histogrammer.publish_histograms()

    exit_requested = False

    while not exit_requested:
        event_buffer = []

        while len(event_buffer) == 0:
            event_buffer = event_source.get_new_data()

            # See if the stop time has been exceeded
            if len(event_buffer) == 0:
                if histogrammer.check_stop_time_exceeded(time.time_ns() // 1_000_000):
                    break

            # Check message queue to see if we should stop
            if not msg_queue.empty():
                msg = msg_queue.get(True)
                if msg == "quit":
                    logging.info("Stopping histogramming process")
                    exit_requested = True
                    break

            if exit_requested:
                break

        if event_buffer:
            histogrammer.add_data(event_buffer)

        histogrammer.publish_histograms(time.time_ns())

        hist_stats = histogrammer.get_histogram_stats()
        logging.info("%s", json.dumps(hist_stats))

        # TODO: How to publish?
        # if self.stats_publisher:
        #     try:
        #         self.stats_publisher.send_histogram_stats(hist_stats)
        #     except Exception as error:
        #         logging.error("Could not publish statistics: %s", error)

        time.sleep(0.1)


class HistogramProcess:
    def __init__(self, configuration, start, stop, simulation=False):
        self.queue = Queue()
        self.process = Process(
            target=process, args=(self.queue, configuration, start, stop, simulation)
        )
        self.process.start()

    def stop_process(self):
        self.queue.put("quit")
