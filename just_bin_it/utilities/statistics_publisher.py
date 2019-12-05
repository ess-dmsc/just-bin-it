import graphyte


class StatisticsPublisher:
    def __init__(self, server, port, prefix, metric):
        """
        Constructor.

        :param server: The Graphite server to send to.
        :param port: The Graphite port to send to.
        :param prefix: The data prefix in Graphite.
        :param metric: The name to serve data in Graphite as.
        """
        self.server = server
        self.port = port
        self.prefix = prefix
        self.metric = metric

        # Initialise the connection
        graphyte.init(server, port=port, prefix=prefix)

    def send_histogram_stats(self, hist_stats):
        for i, stat in enumerate(hist_stats):
            graphyte.send(
                f"{self.metric}{i}",
                stat["sum"],
                timestamp=stat["last_pulse_time"] / 10 ** 9,
            )
