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

        :param histograms: The histograms.
        :param start: When to start histogramming from.
        :param stop: When to histogram until.
        """
        self.histograms = histograms
        self.start = start
        self.stop = stop
        self._stop_time_exceeded = False
        self._started = False
        self._previous_sum = {hist.identifier: 0 for hist in self.histograms}
        self._hist_stats = {hist.identifier: {} for hist in self.histograms}

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
        elif self._started:
            info["state"] = HISTOGRAM_STATES["COUNTING"]
        else:
            info["state"] = HISTOGRAM_STATES["INITIALISED"]

        hist_stats = self._compute_histogram_stats(histogram)
        info.update(hist_stats)

        return info

    def _compute_histogram_stats(self, histogram):
        last_pulse_time = int(histogram.last_pulse_time)
        print(f"last_pulse_time: {last_pulse_time}")
        last_recorded_pulse_time = self._hist_stats[histogram.identifier].get(
            "last_pulse_time", 0
        )
        print(f"last_recorded_pulse_time: {last_recorded_pulse_time}")
        total_counts = int(histogram.data.sum())
        diff = total_counts - self._previous_sum[histogram.identifier]
        ts_diff = (last_pulse_time - last_recorded_pulse_time) / 1e9

        rate = diff / ts_diff if ts_diff > 0 else 0
        if last_pulse_time == last_recorded_pulse_time and diff == 0:
            diff = self._hist_stats[histogram.identifier].get("diff", 0)
            rate = self._hist_stats[histogram.identifier].get("rate", 0)
        self._previous_sum[histogram.identifier] = total_counts

        self._hist_stats[histogram.identifier] = {
            "last_pulse_time": last_pulse_time,
            "sum": total_counts,
            "diff": diff,
            "rate": rate,
        }
        return self._hist_stats[histogram.identifier]

    def clear_histograms(self):
        """
        Clear/zero the histograms but retain the shape etc.
        """
        self._hist_stats = {hist.identifier: {} for hist in self.histograms}
        for hist in self.histograms:
            hist.clear_data()
            self._previous_sum[hist.identifier] = 0

    def get_histogram_stats(self):
        """
        Get the stats for all the histograms.

        :return: List of stats.
        """
        results = []

        for i, hist in enumerate(self.histograms):
            try:
                results.append(
                    {
                        # numpy int64 cannot be converted to JSON.
                        "last_pulse_time": self._hist_stats[hist.identifier][
                            "last_pulse_time"
                        ],
                        "sum": self._hist_stats[hist.identifier]["sum"],
                        "diff": self._hist_stats[hist.identifier]["diff"],
                        "rate": self._hist_stats[hist.identifier]["rate"],
                    }
                )
            except KeyError:
                logging.warning("No stats for histogram %s", hist.identifier)
                results.append({})

        return results

    def set_finished(self):
        self._stop_time_exceeded = True

    def is_finished(self):
        return self._stop_time_exceeded

    def histogram_info(self):
        """
        Get the histograms and their status info.
        """
        for h in self.histograms:
            info = self._generate_info(h)
            logging.info(info)
            yield h, info
