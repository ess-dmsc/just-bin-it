from copy import deepcopy

import mock
import pytest

from just_bin_it.command_actioner import (
    CommandActioner,
    ProcessFactory,
    ResponsePublisher,
)
from just_bin_it.histograms.histogram1d import TOF_1D_TYPE
from just_bin_it.histograms.histogram_process import HistogramProcess

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


class TestProcessFactory:
    def test_creates_process(self):
        process_factory = ProcessFactory()
        process_factory.create({}, 123456, 456789, "::schema::", "::schema::", True)


class TestCommandActioner:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.response_publisher = mock.create_autospec(ResponsePublisher)
        self.process_factory = mock.create_autospec(ProcessFactory)
        self.process_1 = mock.create_autospec(HistogramProcess)
        self.process_2 = mock.create_autospec(HistogramProcess)

        # Put into simulation to avoid kafka checks
        self.actioner = CommandActioner(
            self.response_publisher, True, self.process_factory
        )

    def test_on_invalid_command_without_id_then_error_response_not_sent(self):
        invalid_cmd = deepcopy(CONFIG_CMD)
        invalid_cmd["cmd"] = "not a recognised command"
        self.actioner.handle_command_message(invalid_cmd, [])

        self.response_publisher.send_error_response.assert_not_called()

    def test_on_invalid_command_with_id_error_then_response_sent(self):
        invalid_cmd = deepcopy(CONFIG_CMD)
        invalid_cmd["cmd"] = "not a recognised command"
        invalid_cmd["msg_id"] = "hello"
        self.actioner.handle_command_message(invalid_cmd, [])

        self.response_publisher.send_error_response.assert_called_once()

    def test_on_valid_command_without_id_response_not_sent(self):
        self.actioner.handle_command_message(CONFIG_CMD, [])

        self.response_publisher.send_ack_response.assert_not_called()

    def test_on_valid_command_with_id_response_sent(self):
        cmd = deepcopy(CONFIG_CMD)
        cmd["msg_id"] = "hello"
        self.actioner.handle_command_message(cmd, [])

        self.response_publisher.send_ack_response.assert_called_once()

    def test_on_config_command_new_processes_created_and_started(self):
        self.process_factory.create.return_value = self.process_1

        self.actioner.handle_command_message(CONFIG_CMD, [])

        self.process_factory.create.assert_called_once_with(
            mock.ANY, 1564727596867, 1564727668779, "hs00", "ev42", True
        )
        self.process_1.start.assert_called_once()

    def test_on_config_command_existing_processes_stopped(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message(CONFIG_CMD, processes)

        self.process_1.stop.assert_called_once()
        self.process_2.stop.assert_called_once()

    def test_on_unknown_config_command_existing_processes_not_stopped(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message({"cmd": "::unknown::"}, processes)

        self.process_1.stop.assert_not_called()
        self.process_2.stop.assert_not_called()

    def test_on_stop_command_existing_processes_stopped(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message(STOP_CMD, processes)

        self.process_1.stop.assert_called_once()
        self.process_2.stop.assert_called_once()
        assert len(processes) == 0

    def test_on_reset_command_existing_processes_cleared(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message(RESET_CMD, processes)

        self.process_1.stop.assert_not_called()
        self.process_2.stop.assert_not_called()
        self.process_1.clear.assert_called_once()
        self.process_2.clear.assert_called_once()
