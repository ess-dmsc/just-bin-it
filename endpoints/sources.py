import json
import logging
import time
import math
import numpy as np
from endpoints.serialisation import deserialise_ev42, deserialise_hs00


class SourceException(Exception):
    pass


class TooOldTimeRequestedException(Exception):
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

        for _, records in msgs.items():
            for i in records:
                try:
                    data.append((i.timestamp, i.offset, self._process_record(i.value)))
                except SourceException as error:
                    logging.warning("SourceException: %s", error)  # pragma: no mutate

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

    def seek_to_time(self, requested_time: int):
        """
        Repositions the consumer to the first message >= the specified time.

        Note: this is the Kafka message time.

        :param requested_time: Time in milliseconds.
        :return: The corresponding offset.
        """
        # TODO: Check source?
        lowest_offset, highest_offset = self.consumer.get_offset_range()

        # Kafka uses milliseconds
        offset = self.consumer.offset_for_time(requested_time)

        if offset is None:
            logging.warning(
                "Could not find corresponding offset for requested time, so set position to latest message"
            )
            offset = highest_offset

        if offset == lowest_offset:
            # We've gone back as far as we can.
            raise TooOldTimeRequestedException(
                "Cannot find message time in data as supplied time is too old"
            )  # pragma: no mutate

        self.consumer.seek_by_offset(offset)

        return offset

    def offsets_for_time(self, message_time):
        return self.consumer.offset_for_time(message_time)


class HistogramSource(BaseSource):
    def _process_record(self, record):
        try:
            return deserialise_hs00(record)
        except Exception as error:
            raise SourceException(error)


class SimulatedEventSource:
    def __init__(self, config):
        self.tof_centre = 3000
        self.tof_scale = 1000
        self.det_centre = 50
        self.det_scale = 10

        # Based on the config, guess gaussian settings.
        if "histograms" in config and len(config["histograms"]) > 0:
            low, high = config["histograms"][0]["tof_range"]
            self.tof_centre = (high - low) // 2
            self.tof_scale = self.tof_centre // 5

            if "det_range" in config["histograms"][0]:
                low, high = config["histograms"][0]["det_range"]
                self.det_centre = (high - low) // 2
                self.det_scale = self.det_centre // 5

    def get_new_data(self):
        """
        Generate gaussian data centred around the defined centre.

        :return: The generated data.
        """
        tofs = np.random.normal(self.tof_centre, self.tof_scale, 1000)
        dets = np.random.normal(self.det_centre, self.det_scale, 1000)

        data = {
            "pulse_time": math.floor(time.time() * 10 ** 9),
            "tofs": tofs,
            "det_ids": dets,
            "source": "simulator",
        }
        return [(int(time.time() * 1000), 0, data)]

    def seek_to_time(self, requested_time: int):
        """
        Does nothing.

        This command needs to be available so that the simulated source can be used as
        a like for like replacement for a real source

        :param requested_time: ignored
        :return:
        """
        return 0
