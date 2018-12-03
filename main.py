import argparse
import json
import numpy as np
from kafka_consumer import Consumer
from event_source import EventSource
from histogrammer2d import Histogrammer2d
from histogrammer1d import Histogrammer1d
from histogram_factory import HistogramFactory


def main(brokers, topic, json_config):
    # Extract the configuration from the JSON.
    config = json.loads(json_config)
    # Create the histograms
    histograms = HistogramFactory.generate(config)

    # Initialisation
    ec = Consumer(config["data_brokers"], config["data_topics"])
    es = EventSource(ec)

    buffs = []

    while len(buffs) == 0:
        buffs = es.get_data()

    for hist in histograms:
        for b in buffs:
            x = b["tofs"]
            y = b["det_ids"]
            hist.add_data(x, y)

    # Just for debugging purposes plot the data
    # TODO: remove at a later date
    import matplotlib.pyplot as plt

    h = histograms[0]

    if isinstance(h, Histogrammer1d):
        width = 0.7 * (h.x_edges[1] - h.x_edges[0])
        center = (h.x_edges[:-1] + h.x_edges[1:]) / 2
        plt.bar(center, h.histogram, align="center", width=width)
        plt.show()
    elif isinstance(h, Histogrammer2d):
        fig = plt.figure()
        ax = fig.add_subplot(111, title="Hello")
        x, y = np.meshgrid(h.x_edges, h.y_edges)
        ax.pcolormesh(x, y, h.histogram)
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # TODO:This are commented out for now as we are loading the information from the
    # json config file. When using it for real this will define the broker and
    # topic on which to listen for configuration commands
    # parser.add_argument(
    #     "-b",
    #     "--brokers",
    #     type=str,
    #     nargs="+",
    #     help="the broker addresses",
    #     # required=True,
    # )
    #
    # parser.add_argument(
    #     "-t", "--topic", type=str, help="the information topic", required=True,
    # )

    # This argument is temporary while we are decided on how to configure the
    # histogrammer via Kafka.
    parser.add_argument(
        "-c", "--config", type=str, help="the configuration as JSON", required=True
    )

    args = parser.parse_args()

    with open(args.config, "r") as f:
        json_str = f.read()

    main(None, None, json_str)
