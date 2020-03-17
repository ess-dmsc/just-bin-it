import json
from just_bin_it.endpoints.histogram_sink import HistogramSink


HISTOGRAM_STATES = {
    "COUNTING": "COUNTING",
    "FINISHED": "FINISHED",
    "INITIALISED": "INITIALISED",
}


class Histogrammer:
    def __init__(self, producer, histograms, start=None, stop=None):
        """
        Constructor.

        All times are given in ns since the Unix epoch.

        :param producer: The producer for the sink.
        :param histograms: The histograms.
        :param start: When to start histogramming from.
        :param stop: When to histogram until.
        """
        self.histograms = histograms
        self.hist_sink = HistogramSink(producer)
        self.start = start
        self.stop = stop
        self._stop_time_exceeded = False
        self._stop_publishing = False
        self._started = False
        self._stop_leeway = 5000
        self._previous_sum = [0 for _ in self.histograms]

    def add_data(self, event_buffer, simulation=False):
        """
        Add the event data to the histogram(s).

        :param event_buffer: The new data received.
        :param simulation: Indicates whether in simulation.
        """
        for hist in self.histograms:
            for msg_time, _, msg in event_buffer:
                if self.start:
                    if msg_time < self.start:
                        continue
                if self.stop:
                    if msg_time > self.stop:
                        self._stop_time_exceeded = True
                        continue

                self._started = True

                pt = msg["pulse_time"]
                x = msg["tofs"]
                y = msg["det_ids"]
                src = msg["source"] if not simulation else hist.source
                hist.add_data(pt, x, y, src)

    def publish_histograms(self, timestamp=0):
        """
        Publish histogram data to the histogram sink.

        :param timestamp: The timestamp to put in the message (ns since epoch).
        """
        if self._stop_publishing:
            return

        for h in self.histograms:
            info = self._generate_info(h)
            self.hist_sink.send_histogram(h.topic, h, timestamp, json.dumps(info))

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
            total_counts = hist.data.sum()
            diff = total_counts - self._previous_sum[i]
            self._previous_sum[i] = total_counts
            results.append(
                {
                    "last_pulse_time": hist.last_pulse_time,
                    "sum": total_counts,
                    "diff": diff,
                }
            )

        return results

    def check_stop_time_exceeded(self, timestamp: int):
        """
        Checks whether the stop time has been exceeded, if so
        then stop histogramming.

        :param timestamp: The timestamp to check against.
        :return: True, if exceeded.
        """
        # Do nothing if there is no stop time
        if self.stop is None:
            return False

        # Already stopped
        if self._stop_time_exceeded:
            return True

        # Give it some leeway
        if timestamp > self.stop + self._stop_leeway:
            self._stop_time_exceeded = True

        return self._stop_time_exceeded
