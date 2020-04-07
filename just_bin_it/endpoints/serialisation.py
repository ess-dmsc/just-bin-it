import flatbuffers
import streaming_data_types.histogram_hs00 as hs00
import just_bin_it.fbschemas.ev42.EventMessage as EventMessage
from just_bin_it.exceptions import JustBinItException


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


def serialise_hs00(histogrammer, timestamp: int = 0, info_message: str = ""):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogrammer: The histogrammer containing the histogram to serialise.
    :param timestamp: The timestamp to assign to the histogram.
    :param info_message: Information to write to the 'info' field.
    :return: The raw buffer of the FlatBuffers message.
    """

    dim_metadata = [
        {"bin_boundaries": histogrammer.x_edges, "length": histogrammer.shape[0]}
    ]

    if hasattr(histogrammer, "y_edges"):
        dim_metadata.append(
            {"bin_boundaries": histogrammer.y_edges, "length": histogrammer.shape[1]}
        )

    data = {
        "source": "just-bin-it",
        "timestamp": timestamp,
        "current_shape": histogrammer.shape,
        "dim_metadata": dim_metadata,
        "data": histogrammer.data,
        "info": info_message,
    }

    return hs00.serialise_hs00(data)


def deserialise_ev42(buf):
    """
    Deserialise an ev42 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A dictionary of the deserialised values.
    """
    # Check schema is correct
    schema = get_schema(buf)
    if schema != "ev42":
        raise JustBinItException(f"Incorrect schema, expected ev42 but got {schema}")

    event = EventMessage.EventMessage.GetRootAsEventMessage(buf, 0)

    data = {
        "message_id": event.MessageId(),
        "pulse_time": event.PulseTime(),
        "source": event.SourceName().decode("utf-8"),
        "det_ids": event.DetectorIdAsNumpy(),
        "tofs": event.TimeOfFlightAsNumpy(),
    }
    return data


def serialise_ev42(source_name, message_id, pulse_time, tofs, det_ids):
    file_identifier = b"ev42"

    builder = flatbuffers.Builder(1024)
    source = builder.CreateString(source_name)

    EventMessage.EventMessageStartTimeOfFlightVector(builder, len(tofs))
    # FlatBuffers builds arrays backwards
    for x in reversed(tofs):
        builder.PrependInt32(x)
    tof_data = builder.EndVector(len(tofs))

    EventMessage.EventMessageStartDetectorIdVector(builder, len(det_ids))
    # FlatBuffers builds arrays backwards
    for x in reversed(det_ids):
        builder.PrependInt32(x)
    det_data = builder.EndVector(len(det_ids))

    # Build the actual buffer
    EventMessage.EventMessageStart(builder)
    EventMessage.EventMessageAddDetectorId(builder, det_data)
    EventMessage.EventMessageAddTimeOfFlight(builder, tof_data)
    EventMessage.EventMessageAddPulseTime(builder, pulse_time)
    EventMessage.EventMessageAddMessageId(builder, message_id)
    EventMessage.EventMessageAddSourceName(builder, source)
    data = EventMessage.EventMessageEnd(builder)
    builder.Finish(data)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return buff
