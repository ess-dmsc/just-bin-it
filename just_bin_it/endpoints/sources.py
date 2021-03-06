import json
import logging
import math
import time
from enum import Enum
from typing import Optional

from just_bin_it.endpoints.serialisation import (
    EventData,
    deserialise_ev42,
    deserialise_hs00,
)
from just_bin_it.exceptions import SourceException, TooOldTimeRequestedException
from just_bin_it.utilities.fake_data_generation import generate_fake_data


class StopTimeStatus(Enum):
    UNKNOWN = 0
    EXCEEDED = 1
    NOT_EXCEEDED = 2


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
                    logging.debug("SourceException: %s", error)  # pragma: no mutate
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
    def __init__(
        self,
        consumer,
        start_time: Optional[int],
        stop_time: Optional[int] = None,
        deserialise_function=deserialise_ev42,
    ):
        super().__init__(consumer)
        self.start_time = start_time
        self.stop_time = stop_time
        self.deserialise_function = deserialise_function

    def _process_record(self, record):
        try:
            return self.deserialise_function(record)
        except Exception as error:
            raise SourceException(error)

    def seek_to_start_time(self):
        """
        Repositions the consumer to the first message >= the start time.

        Note: this is the Kafka message time.

        :return: The corresponding offsets.
        """
        offset_ranges = self.consumer.get_offset_range()

        # Kafka uses milliseconds
        offsets = self.consumer.offset_for_time(self.start_time)

        for i, (lowest, highest) in enumerate(offset_ranges):
            if offsets[i] is None:
                logging.warning(
                    "Could not find corresponding offset for start time, so set "
                    "position to latest message"
                )
                offsets[i] = highest

            if offsets[i] == lowest:
                # We've gone back as far as we can.
                raise TooOldTimeRequestedException(
                    "Cannot find start time in the data, either the supplied "
                    "time is too old or there is no data available"
                )  # pragma: no mutate

        self.consumer.seek_by_offsets(offsets)

        return offsets

    def stop_time_exceeded(self):
        """
        Has the defined stop time been exceeded in Kafka.

        :return: A StopTimeStatus.
        """
        if not self.stop_time:
            # If the stop time is not defined then it cannot be exceeded
            return StopTimeStatus.NOT_EXCEEDED
        offsets = self.consumer.offset_for_time(self.stop_time)

        if not any(offsets):
            # If all the offsets are None then the stop_time is later than the
            # latest message in Kafka
            return StopTimeStatus.UNKNOWN
        else:
            # Check to see if the consumer is past the offsets
            positions = self.consumer.get_positions()
            for offset, pos in zip(offsets, positions):
                if offset is not None:
                    if pos < offset:
                        return StopTimeStatus.NOT_EXCEEDED
            return StopTimeStatus.EXCEEDED


class HistogramSource(BaseSource):
    def _process_record(self, record):
        try:
            return deserialise_hs00(record)
        except Exception as error:
            raise SourceException(error)


class SimulatedEventSource:
    def __init__(self, config, start, stop):
        self.tof_range = (0, 100_000_000)
        self.det_range = (1, 512)
        self.num_events = 1000
        self.is_dethist = False
        self.width = 0
        self.height = 0
        self.start = start if start else int(time.time() * 1000)
        self.stop = stop

        if config["type"] == "dethist":
            # Different behaviour for this type of histogram
            self.is_dethist = True
            self.width = config["width"]
            self.height = config["height"]
        else:
            # Based on the config, guess gaussian settings.
            self.tof_range = config["tof_range"]

            if "det_range" in config:
                self.det_range = config["det_range"]

    def get_new_data(self):
        """
        Generate gaussian data centred around the defined centre.

        :return: The generated data.
        """
        if self.is_dethist:
            return self._generate_dethist_data()
        else:
            return self._generate_data()

    def _generate_data(self):
        tofs, dets = generate_fake_data(self.tof_range, self.det_range, self.num_events)
        data = EventData(
            "simulator", 0, math.floor(time.time() * 10 ** 9), tofs, dets, None
        )
        return [(int(time.time() * self.num_events), 0, data)]

    def _generate_dethist_data(self):
        dets = []

        for h in range(self.height):
            _, new_dets = generate_fake_data(
                self.tof_range, (0, self.width), self.num_events
            )
            for det in new_dets:
                dets.append(h * self.width + det)
        data = EventData(
            "simulator", 0, math.floor(time.time() * 10 ** 9), [], dets, None
        )
        return [(int(time.time() * self.num_events), 0, data)]

    def seek_to_start_time(self):
        """
        Does nothing.

        This command needs to be available so that the simulated source can be
        used as a like for like replacement for a real source.

        :return:
        """
        return 0

    def stop_time_exceeded(self):
        current_time_ms = int(time.time() * 1000)
        if self.stop and current_time_ms > self.stop:
            return StopTimeStatus.EXCEEDED

        return StopTimeStatus.NOT_EXCEEDED
