import logging
from endpoints.sources import ConfigSource


class ConfigListener:
    def __init__(self, consumer):
        """
        Constructor.

        :param consumer: The underlying consumer to use
        """
        self.config_source = ConfigSource(consumer)
        self.message = None

    def check_for_messages(self):
        """
        Checks for unconsumed messages.

        :return: True if there is a message available.
        """
        messages = self.config_source.get_new_data()
        if messages:
            # Only interested in the "latest" message
            self.message = messages[-1]
            logging.info("New configuration message received")
            return True
        else:
            # Return whether there is a message waiting
            return self.message is not None

    def consume_message(self):
        """
        :return: The waiting message.
        """
        if self.message:
            msg = self.message
            self.message = None
            # Don't need the timestamp or offset
            return msg[2]
        else:
            raise Exception("No message available")
