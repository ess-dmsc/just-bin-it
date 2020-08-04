import copy

import mock
import pytest

from just_bin_it.endpoints.statistics_publisher import (
    GraphiteSender,
    StatisticsPublisher,
)
from just_bin_it.histograms.histogram_process import HistogramProcess

# The message send to Graphite is of the form:
# (stat name, value, timestamp)

STATISTICS_TEMPLATE = {
    "last_pulse_time": 123 * 10 ** 9,  # in ns
    "sum": 1000,
    "diff": 200,
}


def generate_stats_message(last_pulse_time, counts_sum, counts_diff):
    message = copy.deepcopy(STATISTICS_TEMPLATE)
    message["last_pulse_time"] = last_pulse_time
    message["sum"] = counts_sum
    message["diff"] = counts_diff
    return message


class TestStatisticsPublisher:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.metric = "test-metric"
        self.sender = mock.create_autospec(GraphiteSender)
        self.stats_interval = 1000
        self.publisher = StatisticsPublisher(
            self.sender, self.metric, stats_interval_ms=self.stats_interval
        )

    def test_send_stats_has_correct_contents_with_one_process(self):
        last_pulse_times = (12345, 54321)
        sums = (1999, 2134)
        diffs = (678, 787)

        mock_process = mock.create_autospec(HistogramProcess)
        mock_process.get_stats.return_value = [
            generate_stats_message(last_pulse_times[0] * 10 ** 9, sums[0], diffs[0]),
            generate_stats_message(last_pulse_times[1] * 10 ** 9, sums[1], diffs[1]),
        ]

        histogram_processes = [mock_process]
        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=1234
        )

        calls = [
            mock.call(f"{self.metric}0-0-sum", sums[0], timestamp=last_pulse_times[0]),
            mock.call(
                f"{self.metric}0-0-diff", diffs[0], timestamp=last_pulse_times[0]
            ),
            mock.call(f"{self.metric}0-1-sum", sums[1], timestamp=last_pulse_times[1]),
            mock.call(
                f"{self.metric}0-1-diff", diffs[1], timestamp=last_pulse_times[1]
            ),
        ]
        self.sender.send.assert_has_calls(calls)

    def test_send_stats_has_correct_contents_with_multiple_processes(self):
        last_pulse_times = (12345, 54321, 32432)
        sums = (1999, 2134, 1234)
        diffs = (678, 787, 457)

        histogram_processes = []
        for t, s, d in zip(last_pulse_times, sums, diffs):
            mock_process = mock.create_autospec(HistogramProcess)
            mock_process.get_stats.return_value = [
                generate_stats_message(t * 10 ** 9, s, d)
            ]
            histogram_processes.append(mock_process)

        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=1234 * 10 ** 9
        )

        calls = [
            mock.call(f"{self.metric}0-0-sum", sums[0], timestamp=last_pulse_times[0]),
            mock.call(
                f"{self.metric}0-0-diff", diffs[0], timestamp=last_pulse_times[0]
            ),
            mock.call(f"{self.metric}1-0-sum", sums[1], timestamp=last_pulse_times[1]),
            mock.call(
                f"{self.metric}1-0-diff", diffs[1], timestamp=last_pulse_times[1]
            ),
            mock.call(f"{self.metric}2-0-sum", sums[2], timestamp=last_pulse_times[2]),
            mock.call(
                f"{self.metric}2-0-diff", diffs[2], timestamp=last_pulse_times[2]
            ),
        ]
        self.sender.send.assert_has_calls(calls)

    def test_send_stats_with_no_processes(self):
        histogram_processes = []
        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=1234 * 10 ** 9
        )

        self.sender.send.assert_not_called()

    def test_message_send_before_interval_expired_is_ignored(self):
        last_pulse_times = (12345,)
        sums = (1999,)
        diffs = (678,)

        mock_process = mock.create_autospec(HistogramProcess)
        mock_process.get_stats.return_value = [
            generate_stats_message(last_pulse_times[0] * 10 ** 9, sums[0], diffs[0])
        ]

        histogram_processes = [mock_process]

        # First message is always published
        current_time_ms = 1234
        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=current_time_ms
        )

        # Increase time by less than a full interval
        current_time_ms += self.stats_interval // 2
        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=current_time_ms
        )

        # Messages should only be sent for first "publish"
        assert self.sender.send.call_count == 2

    def test_message_send_after_interval_expired_is_published(self):
        last_pulse_times = (12345,)
        sums = (1999,)
        diffs = (678,)

        mock_process = mock.create_autospec(HistogramProcess)
        mock_process.get_stats.return_value = [
            generate_stats_message(last_pulse_times[0] * 10 ** 9, sums[0], diffs[0])
        ]

        histogram_processes = [mock_process]

        # First message is always published
        current_time_ms = 1234
        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=current_time_ms
        )

        # Increase time by a full interval
        current_time_ms += self.stats_interval
        self.publisher.publish_histogram_stats(
            histogram_processes, current_time_ms=current_time_ms
        )

        # Messages sent for both publish
        assert self.sender.send.call_count == 4
