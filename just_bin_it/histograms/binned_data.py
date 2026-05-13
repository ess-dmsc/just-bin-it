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

    output = np.zeros(
        len(output_edges) - 1, dtype=np.result_type(input_counts.dtype, float)
    )
    needs_float = False

    for input_index, count in enumerate(input_counts):
        input_left = input_edges[input_index]
        input_right = input_edges[input_index + 1]
        input_width = input_right - input_left

        if input_right <= output_edges[0] or input_left >= output_edges[-1]:
            continue

        for output_index in range(len(output)):
            output_left = output_edges[output_index]
            output_right = output_edges[output_index + 1]

            overlap = min(input_right, output_right) - max(input_left, output_left)
            if overlap <= 0:
                continue

            fraction = overlap / input_width
            if fraction != 1:
                needs_float = True
            output[output_index] += count * fraction

    if not needs_float and np.issubdtype(input_counts.dtype, np.integer):
        return output.astype(input_counts.dtype)

    return output
