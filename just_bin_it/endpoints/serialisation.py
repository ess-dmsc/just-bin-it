import numpy as np
import streaming_data_types.dataarray_da00 as da00
import streaming_data_types.eventdata_ev42 as ev42
import streaming_data_types.eventdata_ev44 as ev44
import streaming_data_types.histogram_hs00 as hs00
import streaming_data_types.histogram_hs01 as hs01

from just_bin_it.exceptions import JustBinItException
from just_bin_it.histograms.binned_data import BinnedData

DA00_SIGNAL_NAME = "signal"
DA00_FRAME_TIME_NAME = "frame_time"

DA00_UNIT_FACTORS_TO_NS = {
    "ns": 1,
    "us": 1_000,
    "usec": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
}


def get_schema(buf):
    """
    Extract the schema code embedded in the buffer

    :param buf: The raw buffer of the FlatBuffers message.
    :return: The schema name
    """
    return buf[4:8].decode("utf-8")


def deserialise_hs00(buf):
    """
    Convert flatbuffer into a histogram.

    :param buf:
    :return: dict of histogram information
    """
    try:
        return hs00.deserialise_hs00(buf)
    except Exception as error:
        raise JustBinItException(f"Could not deserialise hs00 buffer: {error}")


def deserialise_hs01(buf):
    """
    Convert flatbuffer into a histogram.

    :param buf:
    :return: dict of histogram information
    """
    try:
        return hs01.deserialise_hs01(buf)
    except Exception as error:
        raise JustBinItException(f"Could not deserialise hs01 buffer: {error}")


def serialise_hs00(histogram, timestamp: int = 0, info_message: str = ""):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogram: The histogram to serialise.
    :param timestamp: The timestamp to assign to the histogram.
    :param info_message: Information to write to the 'info' field.
    :return: The raw buffer of the FlatBuffers message.
    """

    dim_metadata = [{"bin_boundaries": histogram.x_edges, "length": histogram.shape[0]}]

    if hasattr(histogram, "y_edges"):
        dim_metadata.append(
            {"bin_boundaries": histogram.y_edges, "length": histogram.shape[1]}
        )

    data = {
        "source": "just-bin-it",
        "timestamp": timestamp,
        "current_shape": histogram.shape,
        "dim_metadata": dim_metadata,
        "data": histogram.data,
        "info": info_message,
    }

    return hs00.serialise_hs00(data)


def serialise_hs01(histogram, timestamp: int = 0, info_message: str = ""):
    """
    Serialise a histogram as an hs01 FlatBuffers message.

    :param histogram: The histogram to serialise.
    :param timestamp: The timestamp to assign to the histogram.
    :param info_message: Information to write to the 'info' field.
    :return: The raw buffer of the FlatBuffers message.
    """

    dim_metadata = [{"bin_boundaries": histogram.x_edges, "length": histogram.shape[0]}]

    if hasattr(histogram, "y_edges"):
        dim_metadata.append(
            {"bin_boundaries": histogram.y_edges, "length": histogram.shape[1]}
        )

    data = {
        "source": "just-bin-it",
        "timestamp": timestamp,
        "current_shape": histogram.shape,
        "dim_metadata": dim_metadata,
        "data": histogram.data,
        "info": info_message,
    }

    return hs01.serialise_hs01(data)


def deserialise_ev42(buf):
    """
    Deserialise an ev42 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A tuple of the deserialised values.
    """
    try:
        result = ev42.deserialise_ev42(buf)
        return (
            result.source_name,
            result.pulse_time,
            result.time_of_flight,
            result.detector_id,
        )
    except Exception as error:
        raise JustBinItException(f"Could not deserialise ev42 buffer: {error}")


def serialise_ev42(source_name, message_id, pulse_time, tofs, det_ids):
    """
    Serialise into an ev42 FlatBuffers message.

    :param source_name: The source name.
    :param message_id: The message ID.
    :param pulse_time: The pulse_time.
    :param tofs: The time-of-flights for the events.
    :param det_ids: The detector IDs for the events.
    :return: The raw buffer of the FlatBuffers message.
    """
    return ev42.serialise_ev42(source_name, message_id, pulse_time, tofs, det_ids)


def deserialise_ev44(buf):
    """
    Deserialise an ev44 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A tuple of the deserialised values.
    """
    try:
        result = ev44.deserialise_ev44(buf)
        return (
            result.source_name,
            result.reference_time[0],
            result.time_of_flight,
            result.pixel_id,
        )
    except Exception as error:
        raise JustBinItException(f"Could not deserialise ev44 buffer: {error}")


def deserialise_da00(buf):
    """
    Deserialise a da00 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A tuple of the deserialised values.
    """
    try:
        result = da00.deserialise_da00(buf)
        signal = _find_da00_variable(result.data, DA00_SIGNAL_NAME)
        frame_time = _find_da00_variable(result.data, DA00_FRAME_TIME_NAME)
        tof_axis = _find_da00_tof_axis(signal)
        tof_edges = _extract_da00_tof_edges(frame_time, signal.data.shape[tof_axis])
        counts = _extract_da00_counts(signal, tof_axis)
        return (
            result.source_name,
            result.timestamp_ns,
            BinnedData(tof_edges, counts),
            None,
        )
    except Exception as error:
        raise JustBinItException(f"Could not deserialise da00 buffer: {error}")


def serialise_da00(
    source_name,
    timestamp,
    data,
):
    """
    Serialise into a da00 FlatBuffers message.

    :param source_name: The source name.
    :param timestamp: The message timestamp.
    :param data: A list of da00 variable definitions.
    :return: The raw buffer of the FlatBuffers message.
    """
    return da00.serialise_da00(
        source_name,
        timestamp,
        [
            da00.Variable(
                variable["name"],
                _normalise_da00_variable_data(variable["data"]),
                axes=variable.get("axes"),
                shape=variable.get("shape"),
                unit=variable.get("unit"),
                label=variable.get("label"),
                source=variable.get("source"),
            )
            for variable in data
        ],
    )


def _normalise_da00_variable_data(data):
    if isinstance(data, (np.ndarray, str)):
        return data
    return np.asarray(data)


def _find_da00_variable(variables, name):
    for variable in variables:
        if variable.name == name:
            return variable
    raise ValueError(f"Missing da00 variable '{name}'")


def _find_da00_tof_axis(signal):
    if not isinstance(signal.data, np.ndarray):
        raise ValueError("da00 signal must be an array")

    if DA00_FRAME_TIME_NAME not in signal.axes:
        raise ValueError("da00 signal has no frame_time axis")

    if len(signal.axes) != signal.data.ndim:
        raise ValueError("da00 signal axes do not match signal dimensions")

    return signal.axes.index(DA00_FRAME_TIME_NAME)


def _extract_da00_tof_edges(frame_time, tof_bins):
    frame_time_data = np.asarray(frame_time.data)
    if frame_time_data.ndim != 1:
        raise ValueError("da00 frame_time must be one dimensional")

    if len(frame_time_data) == tof_bins + 1:
        edges = frame_time_data
    elif len(frame_time_data) == tof_bins:
        edges = np.insert(frame_time_data, 0, 0)
    else:
        raise ValueError("da00 frame_time length does not match signal ToF bins")

    if frame_time.unit not in DA00_UNIT_FACTORS_TO_NS:
        raise ValueError(f"Unsupported da00 frame_time unit '{frame_time.unit}'")

    edges = edges * DA00_UNIT_FACTORS_TO_NS[frame_time.unit]
    if not np.all(np.diff(edges) > 0):
        raise ValueError("da00 frame_time edges must be strictly increasing")

    return edges


def _extract_da00_counts(signal, tof_axis):
    counts = np.moveaxis(signal.data, tof_axis, 0)
    return counts.reshape(counts.shape[0], -1)


def serialise_ev44(source_name, message_id, pulse_time, tofs, det_ids):
    """
    Serialise into an ev44 FlatBuffers message.

    :param source_name: The source name.
    :param message_id: The message ID.
    :param pulse_time: The pulse_time.
    :param tofs: The time-of-flights for the events.
    :param det_ids: The detector IDs for the events.
    :return: The raw buffer of the FlatBuffers message.
    """
    return ev44.serialise_ev44(
        source_name, message_id, [pulse_time], [0], tofs, det_ids
    )


SCHEMAS_TO_SERIALISERS = {"hs00": serialise_hs00, "hs01": serialise_hs01}
SCHEMAS_TO_DESERIALISERS = {
    "hs00": deserialise_hs00,
    "hs01": deserialise_hs01,
    "ev42": deserialise_ev42,
    "ev44": deserialise_ev44,
    "da00": deserialise_da00,
}
