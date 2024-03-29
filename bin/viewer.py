import os
import sys

import configargparse as argparse
import numpy as np

from just_bin_it.utilities.sasl_utils import (
    add_sasl_commandline_options,
    generate_kafka_security_config,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from just_bin_it.endpoints.kafka_consumer import Consumer
from just_bin_it.endpoints.sources import HistogramSource
from just_bin_it.utilities.plotter import plot_histograms


def convert_for_plotting(histogram_data):
    """
    Convert histogram data to a form for plotting.

    :param histogram_data: The histogram to convert.
    """

    class Histogram:
        pass

    hist = Histogram()

    if len(histogram_data["dim_metadata"]) == 1:
        # 1-D
        hist.x_edges = np.array(histogram_data["dim_metadata"][0]["bin_boundaries"])
    else:
        # 2-D
        hist.x_edges = np.array(histogram_data["dim_metadata"][0]["bin_boundaries"])
        hist.y_edges = np.array(histogram_data["dim_metadata"][1]["bin_boundaries"])

    hist.data = np.array(histogram_data["data"])

    return [hist]


def main(brokers, topic, log_scale_for_2d, kafka_security_config):
    """

    :param brokers: The brokers to listen for data on.
    :param topic: The topic to listen for data on.
    :param log_scale_for_2d: Whether to plot 2D images on a log scale
    :param kafka_security_config: The security settings for Kafka.
    """
    # Create the listener
    hist_consumer = Consumer(brokers, [topic], kafka_security_config)
    hist_source = HistogramSource(hist_consumer)

    buffs = []

    while len(buffs) == 0:
        buffs = hist_source.get_new_data()

    # Only care about the most recent histogram and don't care about kafka timestamps
    _, _, hist_data = buffs[-1]

    hists = convert_for_plotting(hist_data)
    plot_histograms(hists, log_scale_for_2d)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-b",
        "--brokers",
        type=str,
        nargs="+",
        help="the broker addresses",
        required=True,
    )

    required_args.add_argument(
        "-t", "--topic", type=str, help="the histogram data topic", required=True
    )

    required_args.add_argument(
        "-l", "--log-scale", type=bool, help="the histogram data topic", default=False
    )

    add_sasl_commandline_options(parser)

    args = parser.parse_args()

    kafka_security_config = generate_kafka_security_config(
        args.security_protocol,
        args.sasl_mechanism,
        args.sasl_username,
        args.sasl_password,
        args.ssl_cafile,
    )

    main(args.brokers, args.topic, args.log_scale, kafka_security_config)
