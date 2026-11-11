import logging
from collections import deque

from just_bin_it.endpoints.sources import ConfigSource


class ConfigListener:
    def __init__(self, consumer):
        """
        Constructor.

        :param consumer: The underlying consumer to use
        """
        self.config_source = ConfigSource(consumer)
        self.messages = deque()

    def check_for_messages(self):
        """
        Checks for unconsumed messages.

        :return: True if there is a message available.
        """
        messages = self.config_source.get_new_data()
        if messages:
            self.messages.extend(messages)
            logging.info("%s new command message(s) received", len(messages))
        return bool(self.messages)

    def consume_message(self):
        """
        :return: The oldest waiting message.
        """
        if not self.messages:
            raise Exception("No message available")
        # Don't need the timestamp or offset
        return self.messages.popleft()[2]
