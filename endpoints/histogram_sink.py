from endpoints.serialisation import serialise_hs00


class HistogramSink:
    def __init__(self, producer, serialise_function=serialise_hs00):
        """
        Constructor.

        :param producer: The underlying Kafka producer to publish to.
        :param serialise_function: The function to use to serialise the data.
        """
        if producer is None:
            raise Exception("Histogram sink must have a producer")  # pragma: no mutate
        self.producer = producer
        self.serialise_function = serialise_function

    def send_histogram(self, topic, histogram, information=""):
        """
        Send a histogram.

        :param topic: The topic to post to.
        :param histogram: The histogram to send.
        :param information: The message to write to the 'info' field.
        """
        self.producer.publish_message(
            topic, self.serialise_function(histogram, information)
        )
