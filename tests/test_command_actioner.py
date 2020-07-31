from copy import deepcopy

import mock
import pytest

from just_bin_it.command_actioner import CommandActioner, ResponsePublisher

TEST_TOPIC = "topic1"
CONFIG_CMD = {
    "cmd": "config",
    "data_brokers": ["localhost:9092"],
    "data_topics": ["TEST_events"],
    "start": 1564727596867,
    "stop": 1564727668779,
    "histograms": [
        {
            "type": "hist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": "output_topic_for_1d",
            "source": "monitor1",
            "id": "histogram1d",
        }
    ],
}
STOP_CMD = {"cmd": "stop"}
RESET_CMD = {"cmd": "reset_counts"}


class SpyProcess:
    def __init__(self):
        self.stop_called = False
        self.clear_called = False

    def stop(self):
        self.stop_called = True

    def clear(self):
        self.clear_called = True


def create_spy_process(*args, **kwargs):
    return SpyProcess()


class TestCommandActioner:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.response_publisher = mock.create_autospec(ResponsePublisher)
        # Put into simulation to avoid kafka checks
        self.actioner = CommandActioner(
            self.response_publisher, True, create_spy_process
        )
        self.spy_process = SpyProcess()

        self.hist_processes = [self.spy_process]

    def test_on_invalid_command_without_id_then_error_response_not_sent(self):
        invalid_cmd = deepcopy(CONFIG_CMD)
        invalid_cmd["cmd"] = "not a recognised command"
        self.actioner.handle_command_message(invalid_cmd, self.hist_processes)

        self.response_publisher.send_error_response.assert_not_called()

    def test_on_invalid_command_with_id_error_then_response_sent(self):
        invalid_cmd = deepcopy(CONFIG_CMD)
        invalid_cmd["cmd"] = "not a recognised command"
        invalid_cmd["msg_id"] = "hello"
        self.actioner.handle_command_message(invalid_cmd, self.hist_processes)

        self.response_publisher.send_error_response.assert_called_once()

    def test_on_valid_command_without_id_response_not_sent(self):
        self.actioner.handle_command_message(CONFIG_CMD, self.hist_processes)

        self.response_publisher.send_ack_response.assert_not_called()

    def test_on_valid_command_with_id_response_sent(self):
        cmd = deepcopy(CONFIG_CMD)
        cmd["msg_id"] = "hello"
        self.actioner.handle_command_message(cmd, self.hist_processes)

        self.response_publisher.send_ack_response.assert_called_once()

    def test_on_config_command_existing_processes_stopped(self):
        self.actioner.handle_command_message(CONFIG_CMD, self.hist_processes)

        assert self.spy_process.stop_called
        assert len(self.hist_processes) == 1
        # Check the process is not the original - it should be a new one
        assert self.hist_processes[0] != self.spy_process

    def test_on_stop_command_existing_processes_stopped(self):
        self.actioner.handle_command_message(STOP_CMD, self.hist_processes)

        assert self.spy_process.stop_called
        assert len(self.hist_processes) == 0

    def test_on_reset_command_existing_processes_cleared(self):
        self.actioner.handle_command_message(RESET_CMD, self.hist_processes)

        assert self.spy_process.clear_called
        assert len(self.hist_processes) == 1
