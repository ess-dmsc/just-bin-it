import json
from histograms.histogram_factory import HistogramFactory
from endpoints.histogram_sink import HistogramSink

HISTOGRAM_STATES = {
    "COUNTING": "COUNTING",
    "FINISHED": "FINISHED",
    "NOT_STARTED": "NOT_STARTED",
}


def create_histogrammer(producer, configuration):
    """
    Creates a fully configured histogrammer.

    :param producer: The
    :param configuration: The configuration message.
    :return: The created histogrammer.
    """
    histograms = HistogramFactory.generate(configuration)
    start = configuration["start"] if "start" in configuration else None
    stop = configuration["stop"] if "stop" in configuration else None

    # Interval is configured in seconds but needs to be converted to nanoseconds
    interval = None
    if "interval" in configuration:
        interval = configuration["interval"] * 10 ** 9

    return Histogrammer(producer, histograms, start, stop, interval)


class Histogrammer:
    def __init__(self, producer, histograms, start=None, stop=None, interval=None):
        """
        Constructor.

        Preferred method of creation is via the create_histogrammer method.

        All times are given in ns since the Unix epoch.

        Note: interval cannot be defined when start and/or stop defined.

        :param producer: The producer for the sink.
        :param histograms: The histograms.
        :param start: When to start histogramming from.
        :param stop: When to histogram until.
        :param interval: How long to histogram for from "now".
        """
        self.histograms = histograms
        self.hist_sink = HistogramSink(producer)
        self.start = start
        self.stop = stop
        self.interval = interval
        self._stop_time_exceeded = False
        self._stop_publishing = False
        self._started = False

        self._check_inputs()

    def _check_inputs(self):
        if self.interval and (self.start or self.stop):
            raise Exception(
                "Cannot define 'interval' in combination with start and/or stop"
            )

        if self.interval and self.interval < 0:
            raise Exception("Interval cannot be negative")

    def add_data(self, event_buffer, simulation=False):
        """
        Add the event data to the histogram(s).

        :param event_buffer: The new data received.
        :param simulation: Indicates whether in simulation.
        """
        self._started = True

        for hist in self.histograms:
            for b in event_buffer:
                if self.interval and not self.start:
                    # First time determine the start and stop times from the
                    # interval and the pulse read.
                    self.start = b["pulse_time"]
                    self.stop = self.start + self.interval

                if self.start:
                    if b["pulse_time"] < self.start:
                        continue
                if self.stop:
                    if b["pulse_time"] > self.stop:
                        self._stop_time_exceeded = True
                        continue

                pt = b["pulse_time"]
                x = b["tofs"]
                y = b["det_ids"]
                src = b["source"] if not simulation else hist.source
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
        if not self._started:
            info["state"] = HISTOGRAM_STATES["NOT_STARTED"]
        elif self._stop_time_exceeded:
            info["state"] = HISTOGRAM_STATES["FINISHED"]
            self._stop_publishing = True
        else:
            info["state"] = HISTOGRAM_STATES["COUNTING"]
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
