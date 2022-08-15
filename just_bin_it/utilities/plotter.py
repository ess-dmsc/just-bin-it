import numpy as np
from matplotlib import colors


def plot_histograms(histograms, log_scale_for_2d=False):
    """
    Plot the histograms.

    :param histograms: The histograms to plot.
    """
    import matplotlib

    matplotlib.use("Qt5Agg")
    from matplotlib import pyplot as plt

    fig = plt.figure(1)

    # See matplotlib.org for an explanation
    plot_num_base = len(histograms) * 100 + 11

    for i, hist in enumerate(histograms):
        if hasattr(hist, "y_edges"):
            # Is 2-D
            x, y = np.meshgrid(hist.x_edges, hist.y_edges)
            ax = fig.add_subplot(plot_num_base + i)

            # Note: need to transpose the data for display
            if log_scale_for_2d:
                min_value = hist.data.min() if hist.data.min() > 0 else 1
                ax.pcolormesh(
                    x,
                    y,
                    hist.data.T,
                    norm=colors.LogNorm(vmin=min_value, vmax=hist.data.max()),
                )
            else:
                ax.pcolormesh(x, y, hist.data.T, shading="auto")
            # And flip the y-axis to match ESS geometry definition
            ax.invert_yaxis()
        else:
            # Is 1-D
            width = 0.8 * (hist.x_edges[1] - hist.x_edges[0])
            center = (hist.x_edges[:-1] + hist.x_edges[1:]) / 2
            ax = fig.add_subplot(plot_num_base + i)
            ax.bar(center, hist.data, align="center", width=width)

    plt.show()
