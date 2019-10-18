import json
import time
from just_bin_it.histograms.histogram_factory import HistogramFactory
from just_bin_it.endpoints.histogram_sink import HistogramSink

HISTOGRAM_STATES = {
    "COUNTING": "COUNTING",
    "FINISHED": "FINISHED",
    "INITIALISED": "INITIALISED",
}


def create_histogrammer(producer, configuration, current_time=None):
    """
    Creates a fully configured histogrammer.

    :param producer: The
    :param configuration: The configuration message.
    :param current_time: Used to set start time if interval is supplied (ms).
    :return: The created histogrammer.
    """
    histograms = HistogramFactory.generate(configuration)
    start = configuration["start"] if "start" in configuration else None
    stop = configuration["stop"] if "stop" in configuration else None

    # Interval is configured in seconds but needs to be converted to milliseconds
    interval = (
        configuration["interval"] * 10 ** 3 if "interval" in configuration else None
    )

    if interval and (start or stop):
        raise Exception(
            "Cannot define 'interval' in combination with start and/or stop"
        )

    if interval and interval < 0:
        raise Exception("Interval cannot be negative")

    if interval:
        start = int(current_time) if current_time else int(time.time() * 1000)
        stop = start + interval

    return Histogrammer(producer, histograms, start, stop)


class Histogrammer:
    def __init__(self, producer, histograms, start=None, stop=None):
        """
        Constructor.

        Preferred method of creation is via the create_histogrammer method.

        All times are given in ns since the Unix epoch.

        Note: interval cannot be defined when start and/or stop defined.

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
        self._stop_leeway = 2000

    def add_data(self, event_buffer, simulation=False):
        """
        Add the event data to the histogram(s).

        :param event_buffer: The new data received.
        :param simulation: Indicates whether in simulation.
        """
        for hist in self.histograms:
            for msg_time, msg_offset, msg in event_buffer:

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
        for hist in self.histograms:
            hist.clear_data()

    def get_histogram_stats(self):
        """
        Get the stats for all the histograms.

        :return: List of stats.
        """
        results = []

        for h in self.histograms:
            results.append({"last_pulse_time": h.last_pulse_time, "sum": h.data.sum()})

        return results

    def check_stop_time_exceeded(self, timestamp: int):
        """
        Checks whether the stop time has been exceeded, if so it stops the histogramming.

        :param timestamp: The timestamp to check against
        :return: True if exceeded
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
