import json
import logging
import time
import math
import numpy as np
from endpoints.serialisation import deserialise_ev42, deserialise_hs00


class SourceException(Exception):
    pass


class BaseSource:
    def __init__(self, consumer):
        """
        Constructor.

        :param consumer: The underlying consumer.
        """
        if consumer is None:
            raise Exception("Event source must have a consumer")  # pragma: no mutate
        self.consumer = consumer

    def get_new_data(self):
        """
        Get the latest data from the consumer.

        :return: The list of data.
        """
        data = []
        msgs = self.consumer.get_new_messages()

        for topic, records in msgs.items():
            for i in records:
                try:
                    data.append(self._process_record(i.value))
                except SourceException as error:
                    logging.warning(f"SourceException: {error}")  # pragma: no mutate

        return data

    def _process_record(self, record):
        raise NotImplementedError("Processing not implemented.")  # pragma: no mutate


class ConfigSource(BaseSource):
    def _process_record(self, record):
        try:
            return json.loads(record)
        except json.JSONDecodeError as error:
            raise SourceException(error.msg)


class EventSource(BaseSource):
    def __init__(self, consumer, deserialise_function=deserialise_ev42):
        super().__init__(consumer)
        self.deserialise_function = deserialise_function

    def _process_record(self, record):
        try:
            return self.deserialise_function(record)
        except Exception as error:
            raise SourceException(error)

    def seek_to_pulse_time(self, start_time: int):
        """
        Repositions the consumer to the last message before the specified time.

        Note: this is the pulse time not the Kafka message time.

        :param start_time: Time in nanoseconds.
        :return: The offset for the start
        """
        # Kafka gives us a rough position based on the Kafka message timestamp
        # but we need to fine tune to get the real start

        # TODO: Check source?

        # Kafka uses milliseconds, but we use nanoseconds
        offset = self.consumer.seek_by_time(start_time // 1_000_000)

        lowest_offset, highest_offset = self.consumer.get_offset_range()

        data = []
        while not data:
            data = self.get_new_data()

        while True:
            # Seek back one value
            offset -= 1

            if offset < lowest_offset:
                # We've gone back as far as we can.
                raise Exception(
                    "Cannot find start time in data as supplied start time is too old"
                )  # pragma: no mutate

            self.consumer.seek_by_offset(offset)

            data = []
            while not data:
                data = self.get_new_data()

            if int(data[0]["pulse_time"]) < start_time:
                # Move to the first pulse after the start time.
                offset += 1
                break
            elif int(data[0]["pulse_time"]) == start_time:
                break

        # Move to the offset
        self.consumer.seek_by_offset(offset)
        return offset


class HistogramSource(BaseSource):
    def _process_record(self, record):
        try:
            return deserialise_hs00(record)
        except Exception as error:
            raise SourceException(error)


class SimulatedEventSource1D:
    def __init__(self, config):
        self.centre = 3000
        self.scale = 1000

        # Based on the config, guess gaussian settings.
        if "histograms" in config and len(config["histograms"]) > 0:
            low, high = config["histograms"][0]["tof_range"]
            self.centre = (high - low) // 2
            self.scale = self.centre // 5

    def get_new_data(self):
        """
        Generate gaussian data centred around the defined centre.

        :return: The generated data.
        """
        tofs = np.random.normal(self.centre, self.scale, 1000)

        data = {
            "pulse_time": math.floor(time.time() * 10 ** 9),
            "tofs": tofs,
            "det_ids": None,
            "source": "simulator",
        }
        return [data]
