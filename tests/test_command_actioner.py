from copy import deepcopy

import mock
import pytest

from just_bin_it.command_actioner import CommandActioner, ResponsePublisher
from just_bin_it.histograms.histogram1d import TOF_1D_TYPE

TEST_TOPIC = "topic1"
CONFIG_CMD = {
    "cmd": "config",
    "start": 1564727596867,
    "stop": 1564727668779,
    "histograms": [
        {
            "type": TOF_1D_TYPE,
            "data_brokers": ["localhost:9092"],
            "data_topics": ["TEST_events"],
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


class TestCommandActioner:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.response_publisher = mock.create_autospec(ResponsePublisher)
        self.process_factory = mock.create_autospec(ProcessCreator)
        self.process_1 = mock.create_autospec(HistogramProcess)
        self.process_2 = mock.create_autospec(HistogramProcess)
        self.processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = self.processes

        # Put into simulation to avoid kafka checks
        self.actioner = CommandActioner(
            self.response_publisher, True, self.process_factory
        )

    def test_on_invalid_command_without_id_then_error_response_not_sent(self):
        invalid_cmd = deepcopy(CONFIG_CMD)
        invalid_cmd["cmd"] = "not a recognised command"
        self.actioner.handle_command_message(invalid_cmd, self.processes)

        self.response_publisher.send_error_response.assert_not_called()

    def test_on_invalid_command_with_id_error_then_response_sent(self):
        invalid_cmd = deepcopy(CONFIG_CMD)
        invalid_cmd["cmd"] = "not a recognised command"
        invalid_cmd["msg_id"] = "hello"
        self.actioner.handle_command_message(invalid_cmd, self.processes)

        self.response_publisher.send_error_response.assert_called_once()

    def test_on_valid_command_without_id_response_not_sent(self):
        self.actioner.handle_command_message(CONFIG_CMD, self.hist_processes)

        self.response_publisher.send_ack_response.assert_not_called()

    def test_on_valid_command_with_id_response_sent(self):
        cmd = deepcopy(CONFIG_CMD)
        cmd["msg_id"] = "hello"
        self.actioner.handle_command_message(cmd, self.hist_processes)

        self.response_publisher.send_ack_response.assert_called_once()

    def test_on_config_command_new_processes_created(self):
        self.actioner.handle_command_message(CONFIG_CMD, [])

        self.process_factory.create.assert_called_once()

    def test_on_config_command_existing_processes_stopped(self):
        self.actioner.handle_command_message(CONFIG_CMD, self.processes)

        self.process_1.stop.assert_called_once()
        self.process_2.stop.assert_called_once()

    def test_on_stop_command_existing_processes_stopped(self):
        self.actioner.handle_command_message(STOP_CMD, self.processes)

        self.process_1.stop.assert_called_once()
        self.process_2.stop.assert_called_once()
        assert len(self.processes) == 0

    def test_on_reset_command_existing_processes_cleared(self):
        self.actioner.handle_command_message(RESET_CMD, self.processes)

        self.process_1.clear.assert_called_once()
        self.process_2.clear.assert_called_once()
