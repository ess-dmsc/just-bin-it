import numpy as np


def generate_fake_data(tof_range, det_range, num_events):
    """
    Generate Gaussian-based fake event data.

    :param tof_range: The minimum and maximum time-of-flight.
    :param det_range: The minimum and maximum detector IDs (contiguous).
    :param num_events: The number of events to generate.
    :return: The time-of-flights and corresponding detector IDs.
    """
    # Calculate the centres and scaling
    tof_centre = (tof_range[1] - tof_range[0]) // 2
    tof_scale = tof_centre // 5

    det_centre = (det_range[1] - det_range[0]) // 2
    det_scale = det_centre // 5

    # Generate fake data
    tofs = [int(x) for x in np.random.normal(tof_centre, tof_scale, num_events)]
    dets = [int(x) for x in np.random.normal(det_centre, det_scale, num_events)]

    return tofs, dets
