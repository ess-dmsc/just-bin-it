import json
from histograms.histogram_factory import HistogramFactory
from endpoints.histogram_sink import HistogramSink

HISTOGRAM_STATES = {"COUNTING": "COUNTING", "FINISHED": "FINISHED"}


class Histogrammer:
    def __init__(self, producer, configuration):
        """
        Constructor.

        :param producer: The producer for the sink.
        :param configuration: The histogram configuration.
        """
        self.histograms = HistogramFactory.generate(configuration)
        self.hist_sink = HistogramSink(producer)
        self.start = configuration["start"] if "start" in configuration else None
        self.stop = configuration["stop"] if "stop" in configuration else None
        self._stop_time_exceeded = False
        self._stop_publishing = False

    def add_data(self, event_buffer, simulation=False):
        """
        Add the event data to the histogram(s).

        :param event_buffer: The new data received.
        """
        for hist in self.histograms:
            for b in event_buffer:
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

    def publish_histograms(self):
        """
        Publish histogram data to the histogram sink.
        """
        if self._stop_publishing:
            return

        for h in self.histograms:
            info = self._generate_info(h)
            self.hist_sink.send_histogram(h.topic, h, json.dumps(info))

    def _generate_info(self, histogram):
        info = {"id": histogram.id}
        if self._stop_time_exceeded:
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
            results.append({"last_pulse_time": h.last_pulse_time, "sum": sum(h.data)})

        return results
