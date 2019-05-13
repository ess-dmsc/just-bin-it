from endpoints.serialisation import serialise_hs00


class HistogramSink:
    def __init__(self, producer):
        """
        Constructor.

        :param producer: The underlying Kafka producer to publish to.
        """
        if producer is None:
            raise Exception("Event source must have a consumer")  # pragma: no mutate
        self.producer = producer

    def send_histogram(self, topic, histogram):
        """
        Send a histogram.

        :param topic: The topic to post to.
        :param histogram: The histogram to send.
        """
        self.producer.publish_message(topic, serialise_hs00(histogram))
