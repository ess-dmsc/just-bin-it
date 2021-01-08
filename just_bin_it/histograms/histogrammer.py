import json
import logging

HISTOGRAM_STATES = {
    "COUNTING": "COUNTING",
    "FINISHED": "FINISHED",
    "INITIALISED": "INITIALISED",
    "ERROR": "ERROR",
}


class Histogrammer:
    def __init__(self, histogram_sink, histograms, start=None, stop=None):
        """
        Constructor.

        All times are given in ns since the Unix epoch.

        :param histogram_sink: The producer for the sink.
        :param histograms: The histograms.
        :param start: When to start histogramming from.
        :param stop: When to histogram until.
        """
        self.histograms = histograms
        self.hist_sink = histogram_sink
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
            for msg_time, _, msg in event_buffer:
                if self.start:
                    if msg_time < self.start:
                        continue
                if self.stop:
                    if msg_time > self.stop:
                        self._stop_time_exceeded = True
                        continue

                self._started = True
                src = msg.source_name if not simulation else hist.source
                hist.add_data(msg.pulse_time, msg.time_of_flight, msg.detector_id, src)

    def publish_histograms(self, timestamp=0):
        """
        Publish histogram data to the histogram sink.

        :param timestamp: The timestamp to put in the message (ns since epoch).
        """
        if self._stop_publishing:
            return

        for h in self.histograms:
            info = self._generate_info(h)
            logging.info(info)
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
            total_counts = int(hist.data.sum())
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

        :param timestamp: The timestamp to check against in ms.
        :return: True, if exceeded.
        """
        # Do nothing if there is no stop time
        if self.stop is None:
            return False

        # Already stopped
        if self._stop_time_exceeded:
            return True

        # Give it some leeway
        if timestamp > self.stop + self._stop_leeway_ms:
            self._stop_time_exceeded = True

        return self._stop_time_exceeded

    def set_finished(self):
        self._stop_time_exceeded = True

    def send_failure_message(self, timestamp, message):
        for h in self.histograms:
            info = self._generate_info(h)
            info["state"] = HISTOGRAM_STATES["ERROR"]
            info["error_message"] = message
            self.hist_sink.send_histogram(h.topic, h, timestamp, json.dumps(info))
