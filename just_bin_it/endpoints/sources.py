import json
import logging
import math
import time
from typing import Optional

from just_bin_it.endpoints.serialisation import (
    SCHEMAS_TO_DESERIALISERS,
    deserialise_ev42,
    get_schema,
)
from just_bin_it.exceptions import SourceException, TooOldTimeRequestedException
from just_bin_it.histograms.histogram2d_map import MAP_TYPE
from just_bin_it.histograms.histogram2d_roi import ROI_TYPE
from just_bin_it.utilities.fake_data_generation import generate_fake_data


def safe_convert(msg, converter):
    try:
        return msg.timestamp(), msg.offset(), converter(msg.value())
    except Exception as error:
        logging.debug("SourceException: %s", error)  # pragma: no mutate
        return None


def convert_messages(messages, converter):
    return [
        res
        for res in (safe_convert(msg, converter) for msg in messages)
        if res is not None
    ]


class ConfigSource:
    def __init__(
        self,
        consumer,
    ):
        self.consumer = consumer

    def get_new_data(self):
        """
        Get the latest data from the consumer.

        :return: The list of data.
        """
        msgs = self.consumer.get_new_messages()
        return convert_messages(msgs, json.loads)


class EventSource:
    def __init__(
        self,
        consumer,
        start_time: Optional[int],
        stop_time: Optional[int] = None,
        deserialise_function=deserialise_ev42,
    ):
        self.consumer = consumer
        self.start_time = start_time
        self.stop_time = stop_time
        self.deserialise_function = deserialise_function

    def get_new_data(self):
        """
        Get the latest data from the consumer.

        :return: The list of data.
        """
        msgs = self.consumer.get_new_messages()
        return convert_messages(msgs, self.deserialise_function)

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
            if offsets[i] == -1:
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


class HistogramSource:
    def __init__(
        self,
        consumer,
    ):
        self.consumer = consumer

    def get_new_data(self):
        """
        Get the latest data from the consumer.

        :return: The list of data.
        """
        msgs = self.consumer.get_new_messages()
        return convert_messages(msgs, self._process_record)

    def _process_record(self, record):
        try:
            schema = get_schema(record)
            if schema in SCHEMAS_TO_DESERIALISERS:
                return SCHEMAS_TO_DESERIALISERS[schema](record)
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

        if config["type"] == MAP_TYPE:
            self.is_dethist = True
            self.width = config["width"]
            self.height = config["height"]
        elif config["type"] == ROI_TYPE:
            self.is_dethist = True
            self.width = config["width"]
            self.height = config["left_edges"][~0]
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
        data = ("simulator", math.floor(time.time() * 10**9), tofs, dets, None)
        return [(int(time.time() * self.num_events), 0, data)]

    def _generate_dethist_data(self):
        dets = []

        for h in range(self.height):
            _, new_dets = generate_fake_data(
                self.tof_range, (0, self.width), self.num_events
            )
            for det in new_dets:
                dets.append(h * self.width + det)
        data = ("simulator", math.floor(time.time() * 10**9), [], dets, None)
        return [(int(time.time() * self.num_events), 0, data)]

    def seek_to_start_time(self):
        """
        Does nothing.

        This command needs to be available so that the simulated source can be
        used as a like for like replacement for a real source.

        :return:
        """
        return 0
