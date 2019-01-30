from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


class Consumer:
    """
    Consumes the messages from Kafka.

    This contains the least amount of logic because it is hard to effectively
    mock the Kafka side without making the tests trivial or pointless.
    """

    def __init__(self, brokers, topics):
        """
        Constructor.

        :param brokers: The brokers to connect to.
        :param topics: The data topics.
        """
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=brokers)
            self.topics = []
            available_topics = self.consumer.topics()

            for t in topics:
                if t not in available_topics:
                    raise Exception("Requested topic not available")

                topic = TopicPartition(t, 0)
                self.topics.append(topic)
                self.consumer.assign([topic])
                self.consumer.seek_to_end(topic)
        except KafkaError as err:
            raise Exception(err)

    def get_new_messages(self):
        """
        Get any new messages.

        :return: The dict containing the messages.
        """
        data = self.consumer.poll(5)
        print("Consumer position:", self.consumer.position(self.topics[0]))
        return data
