import json
import logging
import math
import time
from just_bin_it.endpoints.serialisation import deserialise_ev42, deserialise_hs00
from just_bin_it.exceptions import SourceException, TooOldTimeRequestedException
from just_bin_it.utilities.fake_data_generation import generate_fake_data


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
        :return: The corresponding offsets.
        """
        offset_ranges = self.consumer.get_offset_range()

        # Kafka uses milliseconds
        offsets = self.consumer.offset_for_time(requested_time)

        for i, (lowest, highest) in enumerate(offset_ranges):
            if offsets[i] is None:
                logging.warning(
                    "Could not find corresponding offset for requested time, so set position to latest message"
                )
                offsets[i] = highest

            if offsets[i] == lowest:
                # We've gone back as far as we can.
                raise TooOldTimeRequestedException(
                    "Cannot find message time in data as supplied time is too old"
                )  # pragma: no mutate

        self.consumer.seek_by_offsets(offsets)

        return offsets


class HistogramSource(BaseSource):
    def _process_record(self, record):
        try:
            return deserialise_hs00(record)
        except Exception as error:
            raise SourceException(error)


class SimulatedEventSource:
    def __init__(self, config):
        self.tof_range = (0, 100_000_000)
        self.det_id = (1, 512)
        self.num_events = 1000

        # Based on the config, guess gaussian settings.
        if "histograms" in config and len(config["histograms"]) > 0:
            self.tof_range = config["histograms"][0]["tof_range"]

            if "det_range" in config["histograms"][0]:
                self.det_range = config["histograms"][0]["det_range"]

    def get_new_data(self):
        """
        Generate gaussian data centred around the defined centre.

        :return: The generated data.
        """
        tofs, dets = generate_fake_data(self.tof_range, self.det_range, self.num_events)

        data = {
            "pulse_time": math.floor(time.time() * 10 ** 9),
            "tofs": tofs,
            "det_ids": dets,
            "source": "simulator",
        }
        return [(int(time.time() * self.num_events), 0, data)]

    def seek_to_time(self, requested_time: int):
        """
        Does nothing.

        This command needs to be available so that the simulated source can be used as
        a like for like replacement for a real source

        :param requested_time: ignored
        :return:
        """
        return 0
