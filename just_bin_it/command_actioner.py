import json
import logging

from just_bin_it.endpoints.kafka_tools import are_kafka_settings_valid
from just_bin_it.exceptions import KafkaException
from just_bin_it.histograms.histogram_factory import parse_config
from just_bin_it.histograms.histogram_process import HistogramProcess


class ProcessFactory:
    def create(
        self,
        config,
        start,
        stop,
        hist_schema,
        event_schema,
        kafka_security_config,
        simulation=False,
    ):
        return HistogramProcess(
            config,
            start,
            stop,
            hist_schema,
            event_schema,
            kafka_security_config,
            simulation=simulation,
        )


class ResponsePublisher:
    def __init__(self, response_producer, response_topic):
        self.response_producer = response_producer
        self.response_topic = response_topic

    def send_ack_response(self, msg_id):
        response = {"msg_id": msg_id, "response": "ACK"}
        self._publish_response(response)

    def send_error_response(self, msg_id, error):
        response = {"msg_id": msg_id, "response": "ERR", "message": str(error)}
        self._publish_response(response)

    def _publish_response(self, response):
        if self.response_topic:
            try:
                self.response_producer.publish_message(
                    self.response_topic, json.dumps(response).encode()
                )
            except KafkaException as error:
                logging.error("Exception when publishing response: %s", error)


class CommandActioner:
    def __init__(
        self,
        response_publisher,
        kafka_security_config,
        simulation=False,
        process_factory=ProcessFactory(),
    ):
        self.response_publisher = response_publisher
        self.kafka_security_config = kafka_security_config
        self.simulation = simulation
        self.process_factory = process_factory

    def handle_command_message(self, message, hist_processes):
        """
        Handle the message received.

        :param message: The message.
        :param hist_processes: The holder for the histogramming processes.
        """
        msg_id = None
        try:
            msg_id = message["msg_id"] if "msg_id" in message else None
            self._handle_command_message(message, hist_processes)
            if msg_id:
                self.response_publisher.send_ack_response(msg_id)
        except Exception as error:
            logging.error("Could not handle configuration: %s", error)
            if msg_id:
                self.response_publisher.send_error_response(msg_id, error)

    def _handle_command_message(self, message, hist_processes):
        if message["cmd"] == "reset_counts":
            logging.info("Reset command received")
            for process in hist_processes:
                process.clear()
        elif message["cmd"] == "stop":
            logging.info("Stop command received")
            self._stop_processes(hist_processes)
        elif message["cmd"] == "config":
            logging.info("Config command received")
            start, stop, hist_configs, hist_schema, event_schema = parse_config(message)

            self._stop_processes(hist_processes)

            try:
                for config in hist_configs:
                    # Check brokers and data topics exist (skip in simulation)
                    if not self.simulation and not are_kafka_settings_valid(
                        config["data_brokers"],
                        config["data_topics"],
                        self.kafka_security_config,
                    ):
                        raise KafkaException("Invalid Kafka settings")

                    process = self.process_factory.create(
                        config,
                        start,
                        stop,
                        hist_schema,
                        event_schema,
                        self.kafka_security_config,
                        self.simulation,
                    )
                    process.start()
                    hist_processes.append(process)
            except Exception as error:
                # If one fails then close any that were started then rethrow
                self._stop_processes(hist_processes)
                raise error
        else:
            raise Exception(f"Unknown command type '{message['cmd']}'")

    def _stop_processes(self, hist_processes):
        """
        Request the processes to stop.
        """
        logging.info("Stopping any existing histogram processes")
        for process in hist_processes:
            try:
                process.stop()
            except Exception as error:
                # Process might have killed itself already
                logging.info("Stopping process failed %s", error)
        hist_processes.clear()
