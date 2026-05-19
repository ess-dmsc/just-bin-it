import numpy as np


class BinnedData:
    def __init__(self, tof_edges, counts):
        self.tof_edges = tof_edges
        self.counts = counts


def rebin_counts(input_edges, input_counts, output_edges):
    """
    Rebin pre-binned counts using fractional bin overlap.
    """
    input_edges = np.asarray(input_edges)
    input_counts = np.asarray(input_counts)
    output_edges = np.asarray(output_edges)

    if len(input_counts) == 0:
        return np.zeros(
            len(output_edges) - 1, dtype=np.result_type(input_counts.dtype, float)
        )

    cumulative_counts = np.concatenate(
        ([0], np.cumsum(input_counts, dtype=np.result_type(input_counts.dtype, float)))
    )
    cumulative_output = np.interp(
        output_edges,
        input_edges,
        cumulative_counts,
        left=0,
        right=cumulative_counts[-1],
    )
    output = np.diff(cumulative_output)

    if np.issubdtype(input_counts.dtype, np.integer) and _bins_are_aligned(
        input_edges, output_edges
    ):
        return output.astype(input_counts.dtype)

    return output


def _bins_are_aligned(input_edges, output_edges):
    clipped_output_edges = np.unique(
        np.clip(output_edges, input_edges[0], input_edges[-1])
    )
    insertion_points = np.searchsorted(input_edges, clipped_output_edges)
    insertion_points = np.clip(insertion_points, 0, len(input_edges) - 1)

    return np.array_equal(input_edges[insertion_points], clipped_output_edges)
