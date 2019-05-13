import json
import logging
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
    def _process_record(self, record):
        try:
            return deserialise_ev42(record)
        except Exception as error:
            raise SourceException(error)


class HistogramSource(BaseSource):
    def _process_record(self, record):
        try:
            return deserialise_hs00(record)
        except Exception as error:
            raise SourceException(error)
