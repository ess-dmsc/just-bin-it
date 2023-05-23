import pytest

from just_bin_it.endpoints.config_listener import ConfigListener
from tests.doubles.consumer import StubConsumer, StubConsumerRecord


class TestConfigListener:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = StubConsumer(["broker1"], ["topic1"])
        self.config_listener = ConfigListener(self.consumer)

    def test_when_no_message_waiting_then_checking_returns_no_message_waiting(self):
        assert not self.config_listener.check_for_messages()

    def test_when_message_waiting_then_checking_returns_that_message_waiting(self):
        mock_record = StubConsumerRecord(0, 0, '"message1"')
        self.consumer.add_messages([mock_record])

        assert self.config_listener.check_for_messages()

    def test_when_waiting_message_not_consumed_checking_again_still_returns_message_waiting(
        self,
    ):
        mock_record = StubConsumerRecord(0, 0, '"message1"')
        self.consumer.add_messages([mock_record])
        self.config_listener.check_for_messages()

        assert self.config_listener.check_for_messages()

    def test_consuming_waiting_message_get_message(self):
        mock_record = StubConsumerRecord(0, 0, '"message1"')
        self.consumer.add_messages([mock_record])
        self.config_listener.check_for_messages()

        msg = self.config_listener.consume_message()
        assert msg == "message1"

    def test_consuming_waiting_message_clears_message_waiting(self):
        mock_record = StubConsumerRecord(0, 0, '"message1"')
        self.consumer.add_messages([mock_record])
        self.config_listener.check_for_messages()

        _ = self.config_listener.consume_message()

        assert not self.config_listener.check_for_messages()

    def test_consuming_waiting_message_gets_latest_message(self):
        mock_record = [StubConsumerRecord(i, i, f'"message{i+1}"') for i in range(3)]
        self.consumer.add_messages(mock_record)
        self.config_listener.check_for_messages()

        msg = self.config_listener.consume_message()
        assert msg == "message3"

    def test_when_no_messages_trying_to_consume_throws(self):
        with pytest.raises(Exception):
            _ = self.config_listener.consume_message()
