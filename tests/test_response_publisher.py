import json

import pytest

from just_bin_it.command_actioner import ResponsePublisher
from tests.doubles.producers import SpyProducer


class TestResponsePublisher:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.producer = SpyProducer()
        self.topic = "TOPIC1"
        self.response_publisher = ResponsePublisher(self.producer, self.topic)

    def test_error_message_has_correct_contents(self):
        msg_id = 1234
        error_msg = "This is an error message!"
        self.response_publisher.send_error_response(msg_id, error_msg)

        assert len(self.producer.messages) == 1
        response = json.loads(self.producer.messages[0][1])
        assert response["response"] == "ERR"
        assert response["msg_id"] == msg_id
        assert response["message"] == error_msg

    def test_ack_message_has_correct_contents(self):
        msg_id = 1234
        self.response_publisher.send_ack_response(msg_id)

        assert len(self.producer.messages) == 1
        response = json.loads(self.producer.messages[0][1])
        assert response["response"] == "ACK"
        assert response["msg_id"] == msg_id
