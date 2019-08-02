import pytest
from tests.mock_consumer import MockConsumer
from endpoints.config_listener import ConfigListener


class TestConfigListener:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.mock_consumer = MockConsumer(["broker1"], ["topic1"])
        self.config_listener = ConfigListener(self.mock_consumer)

    def test_when_no_message_waiting_then_checking_returns_no_message_waiting(self):
        assert not self.config_listener.check_for_messages()

    def test_when_message_waiting_then_checking_returns_that_message_waiting(self):
        self.mock_consumer.add_messages([(0, 0, '"message1"')])

        assert self.config_listener.check_for_messages()

    def test_when_waiting_message_not_consumed_checking_again_still_returns_message_waiting(
        self
    ):
        self.mock_consumer.add_messages([(0, 0, '"message1"')])
        self.config_listener.check_for_messages()

        assert self.config_listener.check_for_messages()

    def test_consuming_waiting_message_get_message(self):
        self.mock_consumer.add_messages([(0, 0, '"message1"')])
        self.config_listener.check_for_messages()

        _, _, msg = self.config_listener.consume_message()
        assert msg == "message1"

    def test_consuming_waiting_message_clears_message_waiting(self):
        self.mock_consumer.add_messages([(0, 0, '"message1"')])
        self.config_listener.check_for_messages()

        _ = self.config_listener.consume_message()

        assert not self.config_listener.check_for_messages()

    def test_consuming_waiting_message_gets_latest_message(self):
        self.mock_consumer.add_messages(
            [(0, 0, '"message1"'), (1, 1, '"message2"'), (2, 2, '"message3"')]
        )
        self.config_listener.check_for_messages()

        _, _, msg = self.config_listener.consume_message()
        assert msg == "message3"

    def test_when_no_messages_trying_to_consume_throws(self):
        with pytest.raises(Exception):
            _ = self.config_listener.consume_message()
