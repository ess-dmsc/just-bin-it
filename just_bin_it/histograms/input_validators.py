import numbers
import re


def is_collection_numeric(data):
    return all((isinstance(x, numbers.Number) for x in data))


def check_tof(tof):
    if not isinstance(tof, (list, tuple)) or len(tof) != 2:
        return False
    if not is_collection_numeric(tof):
        return False
    if tof[0] > tof[1]:
        return False
    return True


def check_det_range(det_range):
    if not isinstance(det_range, (list, tuple)) or len(det_range) != 2:
        return False
    if not is_collection_numeric(det_range):
        return False
    if det_range[0] > det_range[1]:
        return False
    return True


def check_bins(num_bins):
    if isinstance(num_bins, int) and num_bins > 0:
        return True

    if isinstance(num_bins, (list, tuple)) and len(num_bins) == 2:
        if (
            isinstance(num_bins[0], int)
            and num_bins[0] > 0
            and isinstance(num_bins[1], int)
            and num_bins[1] > 0
        ):
            return True

    return False


def check_topic(topic):
    if not isinstance(topic, str):
        return False
    # Matching rules from Kafka documentation
    if not re.match(r"^[a-zA-Z0-9._\-]+$", topic):
        return False
    return True


def check_data_topics(topics):
    if not isinstance(topics, (list, tuple)):
        return False

    return all(check_topic(topic) for topic in topics)


def check_data_brokers(brokers):
    if not isinstance(brokers, (list, tuple)):
        return False

    # For now just check they are strings
    return all(isinstance(broker, str) for broker in brokers)


def check_id(hist_id):
    return isinstance(hist_id, str)


def check_source(source):
    return isinstance(source, str)
