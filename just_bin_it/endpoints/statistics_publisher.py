import logging

import graphyte


class GraphiteSender:
    def __init__(self, server, port, prefix):
        self.sender = graphyte.Sender(server, port=port, prefix=prefix)

    def send(self, name, value, timestamp):
        self.sender.send(name, value, timestamp)


class StatisticsPublisher:
    def __init__(self, sender, metric, stats_interval_ms=1000):
        """
        Constructor.

        :param sender: The object which sends stats to Graphite
        :param metric: The name to serve data in Graphite as.
        :param stats_interval_ms: How often to publish.
        """
        self.sender = sender
        self.metric = metric
        self.stats_interval_ms = stats_interval_ms
        self.next_publish_time_ms = 0

    def publish_histogram_stats(self, histogram_processes, current_time_ms):
        if current_time_ms < self.next_publish_time_ms:
            return

        for i, process in enumerate(histogram_processes):
            try:
                stats = process.get_stats()
                if stats:
                    self._send_stats(stats, i)
            except Exception as error:
                logging.error(
                    "Could not publish statistics for process %s: %s", i, error
                )
        self._update_publish_time(current_time_ms)

    def _update_publish_time(self, current_time):
        self.next_publish_time_ms = current_time + self.stats_interval_ms
        # Round to nearest whole interval
        self.next_publish_time_ms -= self.next_publish_time_ms % self.stats_interval_ms

    def _send_stats(self, hist_stats, process_index):
        for i, stat in enumerate(hist_stats):
            # Convert stats from ns to s
            time_stamp = stat["last_pulse_time"] / 10**9

            self.sender.send(
                f"{self.metric}{process_index}-{i}-sum",
                stat["sum"],
                timestamp=time_stamp,
            )
            self.sender.send(
                f"{self.metric}{process_index}-{i}-diff",
                stat["diff"],
                timestamp=time_stamp,
            )
