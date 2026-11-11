import time
from copy import deepcopy
from unittest import mock

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
            self.response_publisher, {}, True, self.process_factory
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
            mock.ANY, 1564727596867, 1564727668779, "hs00", "ev42", {}, True
        )
        self.process_1.start.assert_called_once()

    def test_on_config_command_per_histogram_input_schema_is_used(self):
        cmd = deepcopy(CONFIG_CMD)
        cmd["input_schema"] = "ev44"
        cmd["histograms"].append(deepcopy(cmd["histograms"][0]))
        cmd["histograms"][0]["input_schema"] = "da00"
        cmd["histograms"][1]["topic"] = "output_topic_for_1d_2"
        self.process_factory.create.side_effect = [self.process_1, self.process_2]

        self.actioner.handle_command_message(cmd, [])

        assert self.process_factory.create.call_args_list[0] == mock.call(
            mock.ANY, 1564727596867, 1564727668779, "hs00", "da00", {}, True
        )
        assert self.process_factory.create.call_args_list[1] == mock.call(
            mock.ANY, 1564727596867, 1564727668779, "hs00", "ev44", {}, True
        )
        self.process_1.start.assert_called_once()
        self.process_2.start.assert_called_once()

    def test_on_config_command_existing_processes_stopped(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message(CONFIG_CMD, processes)

        self.process_1.request_stop.assert_called_once()
        self.process_2.request_stop.assert_called_once()
        self.process_1.wait_until_stopped.assert_called_once()
        self.process_2.wait_until_stopped.assert_called_once()

    def test_on_unknown_config_command_existing_processes_not_stopped(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message({"cmd": "::unknown::"}, processes)

        self.process_1.request_stop.assert_not_called()
        self.process_2.request_stop.assert_not_called()

    def test_on_stop_command_existing_processes_stopped(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message(STOP_CMD, processes)

        self.process_1.request_stop.assert_called_once()
        self.process_2.request_stop.assert_called_once()
        self.process_1.wait_until_stopped.assert_called_once()
        self.process_2.wait_until_stopped.assert_called_once()
        assert len(processes) == 0

    def test_on_stop_command_every_process_is_asked_before_any_is_waited_for(self):
        calls = mock.Mock()
        calls.attach_mock(self.process_1, "p1")
        calls.attach_mock(self.process_2, "p2")

        self.actioner.handle_command_message(STOP_CMD, [self.process_1, self.process_2])

        assert calls.mock_calls == [
            mock.call.p1.request_stop(),
            mock.call.p2.request_stop(),
            mock.call.p1.wait_until_stopped(mock.ANY),
            mock.call.p2.wait_until_stopped(mock.ANY),
        ]

    def test_a_process_that_fails_to_stop_does_not_stop_the_others(self):
        self.process_1.request_stop.side_effect = Exception("already dead")
        self.process_1.wait_until_stopped.side_effect = Exception("already dead")
        processes = [self.process_1, self.process_2]

        self.actioner.handle_command_message(STOP_CMD, processes)

        self.process_2.request_stop.assert_called_once()
        self.process_2.wait_until_stopped.assert_called_once()
        assert len(processes) == 0

    def test_on_reset_command_existing_processes_cleared(self):
        processes = [self.process_1, self.process_2]
        self.process_factory.create.side_effect = processes

        self.actioner.handle_command_message(RESET_CMD, processes)

        self.process_1.request_stop.assert_not_called()
        self.process_2.request_stop.assert_not_called()
        self.process_1.clear.assert_called_once()
        self.process_2.clear.assert_called_once()


class TestCommandActionerStopDeadline:
    def test_stuck_processes_share_one_stop_deadline(self):
        actioner = CommandActioner(
            mock.create_autospec(ResponsePublisher), {}, True, mock.Mock()
        )
        waits = []

        def stuck(timeout):
            waits.append(timeout)
            time.sleep(timeout)

        processes = [mock.create_autospec(HistogramProcess) for _ in range(3)]
        for process in processes:
            process.wait_until_stopped.side_effect = stuck

        with mock.patch("just_bin_it.command_actioner.STOP_TIMEOUT_S", 0.3):
            start = time.monotonic()
            actioner.handle_command_message({"cmd": "stop"}, processes)

        assert time.monotonic() - start < 0.8
        assert waits[0] == pytest.approx(0.3, abs=0.05)
        assert waits[1] == waits[2] == 0.1


class TestCommandActionerKafkaCheck:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.response_publisher = mock.create_autospec(ResponsePublisher)
        self.process_factory = mock.create_autospec(ProcessFactory)
        self.actioner = CommandActioner(
            self.response_publisher, {}, False, self.process_factory
        )

    def _config_with(self, brokers_and_topics):
        cmd = deepcopy(CONFIG_CMD)
        template = cmd["histograms"][0]
        cmd["histograms"] = []
        for i, (brokers, topics) in enumerate(brokers_and_topics):
            hist = deepcopy(template)
            hist["data_brokers"] = brokers
            hist["data_topics"] = topics
            hist["topic"] = f"output_{i}"
            hist["id"] = f"hist_{i}"
            cmd["histograms"].append(hist)
        return cmd

    def test_each_set_of_brokers_is_checked_once_for_all_its_topics(self):
        cmd = self._config_with(
            [
                (["broker1"], ["bank0"]),
                (["broker1"], ["bank1"]),
                (["broker1"], ["bank0"]),
                (["broker2"], ["monitors"]),
            ]
        )

        with mock.patch(
            "just_bin_it.command_actioner.are_kafka_settings_valid",
            return_value=True,
        ) as check:
            self.actioner.handle_command_message(cmd, [])

        assert check.call_args_list == [
            mock.call(["broker1"], ["bank0", "bank1"], {}),
            mock.call(["broker2"], ["monitors"], {}),
        ]
        assert self.process_factory.create.call_count == 4

    def test_invalid_kafka_settings_start_no_processes_and_send_error(self):
        cmd = self._config_with([(["broker1"], ["missing"])])
        cmd["msg_id"] = "abc"

        with mock.patch(
            "just_bin_it.command_actioner.are_kafka_settings_valid",
            return_value=False,
        ):
            self.actioner.handle_command_message(cmd, [])

        self.process_factory.create.assert_not_called()
        self.response_publisher.send_error_response.assert_called_once()
        self.response_publisher.send_ack_response.assert_not_called()
