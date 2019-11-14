import numpy as np


def plot_histograms(histograms):
    """
    Plot the histograms.

    :param histograms: The histograms to plot.
    """
    import matplotlib

    matplotlib.use("TkAgg")
    from matplotlib import pyplot as plt

    fig = plt.figure(1)

    # See matplotlib.org for an explanation
    plot_num_base = len(histograms) * 100 + 11

    for i, hist in enumerate(histograms):
        if hasattr(hist, "y_edges"):
            # Is 2-D
            x, y = np.meshgrid(hist.x_edges, hist.y_edges)
            ax = fig.add_subplot(plot_num_base + i)
            # Need to transpose the data for display
            ax.pcolormesh(x, y, hist.data.T)
        else:
            # Is 1-D
            width = 0.8 * (hist.x_edges[1] - hist.x_edges[0])
            center = (hist.x_edges[:-1] + hist.x_edges[1:]) / 2
            ax = fig.add_subplot(plot_num_base + i)
            ax.bar(center, hist.data, align="center", width=width)

    plt.show()
