import logging

from just_bin_it.histograms.binned_data import BinnedData

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

        A message is histogrammed if its pulse time is within [start, stop].

        :param histograms: The histograms.
        :param start: When to start histogramming from (ms since the Unix epoch).
        :param stop: When to histogram until (ms since the Unix epoch).
        """
        self.histograms = histograms
        self.start = start
        self.stop = stop
        self._stop_time_exceeded = False
        self._no_data_after_stop = False
        self._started = False
        self._last_counted_pulse_ns = None
        self._stop_overrun_ms = None
        self._previous_sum = {hist.identifier: 0 for hist in self.histograms}
        self._hist_stats = {hist.identifier: {} for hist in self.histograms}

    def add_data(self, event_buffer, simulation=False):
        """
        Add the event data to the histogram(s).

        :param event_buffer: The new data received.
        :param simulation: Indicates whether in simulation.
        """
        start_ns = self.start * 1_000_000 if self.start else None
        stop_ns = self.stop * 1_000_000 if self.stop else None
        for hist in self.histograms:
            for _, _, msg in event_buffer:
                pulse_time = msg[1]
                src = msg[0] if not simulation else hist.source
                if start_ns is not None and pulse_time < start_ns:
                    continue
                if stop_ns is not None and pulse_time > stop_ns:
                    # Only the histogram's own source ends the window
                    if not hist.source or src == hist.source:
                        self._stop_time_exceeded = True
                    continue

                self._started = True
                if (not hist.source or src == hist.source) and (
                    self._last_counted_pulse_ns is None
                    or pulse_time > self._last_counted_pulse_ns
                ):
                    self._last_counted_pulse_ns = pulse_time
                if isinstance(msg[2], BinnedData):
                    self._add_binned_data(hist, msg[1], msg[2], src)
                else:
                    hist.add_data(msg[1], msg[2], msg[3], src)

    def _add_binned_data(self, hist, pulse_time, binned_data, source):
        if not hasattr(hist, "add_binned_data"):
            logging.warning(
                "Histogram %s does not support binned data", hist.identifier
            )  # pragma: no mutate
            return

        hist.add_binned_data(pulse_time, binned_data, source)

    def _generate_info(self, histogram):
        info = {"id": histogram.identifier}
        if self.start:
            info["start"] = self.start

        if self.stop:
            info["stop"] = self.stop

        if self._stop_overrun_ms:
            info["stop_overrun_ms"] = self._stop_overrun_ms

        if self._no_data_after_stop:
            info["no_data_after_stop"] = True

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
        last_recorded_pulse_time = self._hist_stats[histogram.identifier].get(
            "last_pulse_time", 0
        )
        total_counts = int(self._counts_sum(histogram))
        diff = total_counts - self._previous_sum[histogram.identifier]
        if last_pulse_time == last_recorded_pulse_time and diff == 0:
            # If we are resending "duplicate" data,
            # we don't want to update the stats.
            diff = self._hist_stats[histogram.identifier].get("diff", 0)
            rate = self._hist_stats[histogram.identifier].get("rate", 0)
        else:
            ts_diff = (last_pulse_time - last_recorded_pulse_time) / 1e9
            rate = diff / ts_diff if ts_diff > 0 else 0

        self._previous_sum[histogram.identifier] = total_counts

        self._hist_stats[histogram.identifier] = {
            "last_pulse_time": last_pulse_time,
            "sum": total_counts,
            "diff": diff,
            "rate": rate,
        }
        return self._hist_stats[histogram.identifier]

    def _counts_sum(self, histogram):
        counts_sum = getattr(histogram, "counts_sum", None)
        if counts_sum:
            return counts_sum()
        return histogram.data.sum()

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

    def set_stop_time(self, stop):
        """
        Change the stop time while histogramming. Only an earlier stop time
        is used, data already histogrammed after it is reported as
        "stop_overrun_ms".

        :param stop: The new stop time (ms since the Unix epoch).
        """
        if self.stop is not None and stop >= self.stop:
            logging.info(
                "Stop time %s is not before the current one %s, keeping it",
                stop,
                self.stop,
            )  # pragma: no mutate
            return
        self.stop = stop
        stop_ns = stop * 1_000_000
        if (
            self._last_counted_pulse_ns is not None
            and self._last_counted_pulse_ns > stop_ns
        ):
            self._stop_overrun_ms = (self._last_counted_pulse_ns - stop_ns) / 1e6
            logging.warning(
                "Data up to %s ms after the new stop time was already histogrammed",
                self._stop_overrun_ms,
            )
            self._stop_time_exceeded = True

    def set_finished(self):
        self._stop_time_exceeded = True

    def set_no_data_after_stop(self):
        """
        The window ended by the leeway, without a pulse after the stop time.
        """
        self._no_data_after_stop = True

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
