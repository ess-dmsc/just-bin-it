import logging

HISTOGRAM_STATES = {
    "COUNTING": "COUNTING",
    "FINISHED": "FINISHED",
    "INITIALISED": "INITIALISED",
    "ERROR": "ERROR",
}


class Histogrammer:
    def __init__(self, histograms, start=None, stop=None):
        """
        Constructor.

        All times are given in ns since the Unix epoch.

        :param histogram_sink: The producer for the sink.
        :param histograms: The histograms.
        :param start: When to start histogramming from.
        :param stop: When to histogram until.
        """
        self.histograms = histograms
        self.start = start
        self.stop = stop
        self._stop_time_exceeded = False
        self._stop_publishing = False
        self._started = False
        self._stop_leeway_ms = 5000
        self._previous_sum = [0 for _ in self.histograms]

    def add_data(self, event_buffer, simulation=False):
        """
        Add the event data to the histogram(s).

        :param event_buffer: The new data received.
        :param simulation: Indicates whether in simulation.
        """
        for hist in self.histograms:
            for (_, msg_time), _, msg in event_buffer:
                if self.start:
                    if msg_time < self.start:
                        continue
                if self.stop:
                    if msg_time > self.stop:
                        self._stop_time_exceeded = True
                        continue

                self._started = True
                src = msg[0] if not simulation else hist.source
                hist.add_data(msg[1], msg[2], msg[3], src)

    def _generate_info(self, histogram):
        info = {"id": histogram.identifier}
        if self.start:
            info["start"] = self.start

        if self.stop:
            info["stop"] = self.stop

        if self._stop_time_exceeded:
            info["state"] = HISTOGRAM_STATES["FINISHED"]
            self._stop_publishing = True
        elif self._started:
            info["state"] = HISTOGRAM_STATES["COUNTING"]
        else:
            info["state"] = HISTOGRAM_STATES["INITIALISED"]

        return info

    def clear_histograms(self):
        """
        Clear/zero the histograms but retain the shape etc.
        """
        for i, hist in enumerate(self.histograms):
            hist.clear_data()
            self._previous_sum[i] = 0

    def get_histogram_stats(self):
        """
        Get the stats for all the histograms.

        :return: List of stats.
        """
        results = []

        for i, hist in enumerate(self.histograms):
            total_counts = int(hist.data.sum())
            diff = total_counts - self._previous_sum[i]
            self._previous_sum[i] = total_counts
            results.append(
                {
                    # numpy int64 cannot be converted to JSON.
                    "last_pulse_time": int(hist.last_pulse_time),
                    "sum": total_counts,
                    "diff": diff,
                }
            )

        return results

    def set_finished(self):
        self._stop_time_exceeded = True

    def is_finished(self):
        return self._stop_time_exceeded

    def histogram_info(self):
        for h in self.histograms:
            info = self._generate_info(h)
            logging.info(info)
            yield h, info
