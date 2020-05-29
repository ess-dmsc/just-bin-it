import logging
import graphyte


class StatisticsPublisher:
    def __init__(self, server, port, prefix, metric, stats_interval_ms=1000):
        """
        Constructor.

        :param server: The Graphite server to send to.
        :param port: The Graphite port to send to.
        :param prefix: The data prefix in Graphite.
        :param metric: The name to serve data in Graphite as.
        :param stats_interval_ms: How often to publish.
        """
        self.server = server
        self.port = port
        self.prefix = prefix
        self.metric = metric
        self.stats_interval_ms = stats_interval_ms
        self.time_to_publish_stats = 0

        # Initialise the connection
        graphyte.init(server, port=port, prefix=prefix)

    def publish_histogram_stats(self, histogram_processes, current_time):
        if current_time // 1_000_000 < self.time_to_publish_stats:
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
        self._update_publish_time(current_time)

    def _update_publish_time(self, current_time):
        self.time_to_publish_stats = current_time // 1_000_000 + self.stats_interval_ms
        self.time_to_publish_stats -= (
            self.time_to_publish_stats % self.stats_interval_ms
        )

    def _send_stats(self, hist_stats, process_index):
        for i, stat in enumerate(hist_stats):
            graphyte.send(
                f"{self.metric}{process_index}-{i}-sum",
                stat["sum"],
                timestamp=stat["last_pulse_time"] / 10 ** 9,
            )
            graphyte.send(
                f"{self.metric}{process_index}-{i}-diff",
                stat["diff"],
                timestamp=stat["last_pulse_time"] / 10 ** 9,
            )
