"""Map events to initialised bin edges and accumulate counts."""

import numpy as np


def accumulate_counts(histogram, indices):
    """Accumulate flat output indices, retaining every repeated event.

    Dense bincount is useful when its temporary is no larger than the index
    array. Sparse messages update the existing storage without a bin-sized copy.
    """
    if indices.size == 0:
        return
    counts = histogram.reshape(-1)
    if counts.size <= indices.size:
        counts += np.bincount(indices, minlength=counts.size)
    else:
        np.add.at(counts, indices, counts.dtype.type(1))


class BinMapper:
    def __init__(self, bins, value_range):
        # This also preserves NumPy's expansion of equal-ended ranges.
        self.edges = np.histogram_bin_edges([], bins=bins, range=value_range)
        self.size = len(self.edges) - 1
        widths = np.diff(self.edges)
        self._regular = np.ndim(bins) == 0 and np.all(widths > 0)
        self._integer_step = None
        if (
            self.size > 0
            and -(2**53) <= self.edges[0] < self.edges[-1] <= 2**53
            and self.edges[0] == int(self.edges[0])
            and widths[0] > 0
            and widths[0] == int(widths[0])
            and np.all(widths == widths[0])
        ):
            self._integer_step = int(widths[0])

    def contains(self, values):
        return (values >= self.edges[0]) & (values <= self.edges[-1])

    def indices(self, values):
        """Return indices for values already filtered with ``contains``."""
        if self._integer_step is not None and values.dtype.kind in "iu":
            # Widen before subtraction/division: ev44 uses int32, ev42 uint32.
            indices = (
                values.astype(np.int64) - int(self.edges[0])
            ) // self._integer_step
            return np.minimum(indices, self.size - 1).astype(np.intp, copy=False)

        if not self._regular:
            indices = np.searchsorted(self.edges, values, side="right") - 1
            return np.minimum(indices, self.size - 1)

        positions = (values.astype(np.float64) - self.edges[0]) / (
            self.edges[-1] - self.edges[0]
        )
        indices = np.clip((positions * self.size).astype(np.intp), 0, self.size - 1)
        # Arithmetic can land either side of an internal edge by one ULP.
        # Compare against the actual published boundaries, including the last edge.
        indices -= values < self.edges[indices]
        indices += (indices < self.size - 1) & (values >= self.edges[indices + 1])
        return indices
