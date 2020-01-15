import numpy as np
import flatbuffers
import just_bin_it.fbschemas.ev42.EventMessage as EventMessage
import just_bin_it.fbschemas.hs00.ArrayDouble as ArrayDouble
import just_bin_it.fbschemas.hs00.DimensionMetaData as DimensionMetaData
import just_bin_it.fbschemas.hs00.EventHistogram as EventHistogram
from just_bin_it.fbschemas.hs00.Array import Array
from just_bin_it.exceptions import JustBinItException


def get_schema(buf):
    """
    Extract the schema code embedded in the buffer

    :param buf: The raw buffer of the FlatBuffers message.
    :return: The schema name
    """
    return buf[4:8].decode("utf-8")


def deserialise_ev42(buf):
    """
    Deserialise an ev42 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A dictionary of the deserialised values.
    """
    # Check schema is correct
    schema = get_schema(buf)
    if schema != "ev42":
        raise JustBinItException(f"Incorrect schema: expected ev42 but got {schema}")

    event = EventMessage.EventMessage.GetRootAsEventMessage(buf, 0)

    data = {
        "message_id": event.MessageId(),
        "pulse_time": event.PulseTime(),
        "source": event.SourceName().decode("utf-8"),
        "det_ids": event.DetectorIdAsNumpy(),
        "tofs": event.TimeOfFlightAsNumpy(),
    }
    return data


def deserialise_hs00(buf):
    """
    Convert flatbuffer into a histogram.

    :param buf:
    :return: dict of histogram information
    """
    # Check schema is correct
    if get_schema(buf) != "hs00":
        raise JustBinItException(
            f"Incorrect schema: expected hs00 but got {get_schema(buf)}"
        )

    event_hist = EventHistogram.EventHistogram.GetRootAsEventHistogram(buf, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_fb = event_hist.DimMetadata(i).BinBoundaries()

        # Get bins
        temp = ArrayDouble.ArrayDouble()
        temp.Init(bins_fb.Bytes, bins_fb.Pos)
        bins = temp.ValueAsNumpy()

        # Get type
        if event_hist.DimMetadata(i).BinBoundariesType() == Array.ArrayDouble:
            bin_type = np.float64
        else:
            raise TypeError("Type of the bin boundaries is incorrect")

        hist_info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": bins.tolist(),
            "type": bin_type,
        }
        dims.append(hist_info)

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError("Type of the data array is incorrect")  # pragma: no mutate

    data_fb = event_hist.Data()
    temp = ArrayDouble.ArrayDouble()
    temp.Init(data_fb.Bytes, data_fb.Pos)
    data = temp.ValueAsNumpy()
    shape = event_hist.CurrentShapeAsNumpy().tolist()

    hist = {
        "source": event_hist.Source().decode("utf-8"),
        "timestamp": event_hist.Timestamp(),
        "shape": shape,
        "dims": dims,
        "data": data.reshape(shape),
        "info": event_hist.Info().decode("utf-8")
        if event_hist.Info()
        else "",  # pragma: no mutate
    }
    return hist


def _serialise_metadata(builder, edges, length):
    ArrayDouble.ArrayDoubleStartValueVector(builder, len(edges))
    # FlatBuffers builds arrays backwards
    for x in reversed(edges):
        builder.PrependFloat64(x)
    bins = builder.EndVector(len(edges))
    # Add the bins
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, bins)
    pos_bin = ArrayDouble.ArrayDoubleEnd(builder)

    DimensionMetaData.DimensionMetaDataStart(builder)
    DimensionMetaData.DimensionMetaDataAddLength(builder, length)
    DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, pos_bin)
    DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, Array.ArrayDouble)
    return DimensionMetaData.DimensionMetaDataEnd(builder)


def serialise_hs00(histogrammer, timestamp: int = 0, info_message: str = ""):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogrammer: The histogrammer containing the histogram to serialise.
    :param timestamp: The timestamp to assign to the histogram.
    :param info_message: Information to write to the 'info' field.
    :return: The raw buffer of the FlatBuffers message.
    """
    file_identifier = b"hs00"

    builder = flatbuffers.Builder(1024)
    source = builder.CreateString("just-bin-it")
    info = builder.CreateString(info_message)

    # Build shape array
    rank = len(histogrammer.shape)
    EventHistogram.EventHistogramStartCurrentShapeVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for s in reversed(histogrammer.shape):
        builder.PrependUint32(s)
    shape = builder.EndVector(rank)

    # Build dimensions metadata
    # Build the x bins vector
    metadata = [
        _serialise_metadata(builder, histogrammer.x_edges, histogrammer.shape[0])
    ]

    # Build the y bins vector, if present
    if hasattr(histogrammer, "y_edges"):
        metadata.append(
            _serialise_metadata(builder, histogrammer.y_edges, histogrammer.shape[1])
        )

    EventHistogram.EventHistogramStartDimMetadataVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for m in reversed(metadata):
        builder.PrependUOffsetTRelative(m)
    metadata_vector = builder.EndVector(rank)

    # Build the data
    data_len = len(histogrammer.data)
    if len(histogrammer.shape) == 2:
        # 2-D data will be flattened into one array
        data_len = histogrammer.shape[0] * histogrammer.shape[1]

    ArrayDouble.ArrayDoubleStartValueVector(builder, data_len)
    # FlatBuffers builds arrays backwards
    for x in reversed(histogrammer.data.flatten()):
        builder.PrependFloat64(x)
    data = builder.EndVector(data_len)
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, data)
    pos_data = ArrayDouble.ArrayDoubleEnd(builder)

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    EventHistogram.EventHistogramAddInfo(builder, info)
    EventHistogram.EventHistogramAddData(builder, pos_data)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    EventHistogram.EventHistogramAddTimestamp(builder, timestamp)
    EventHistogram.EventHistogramAddSource(builder, source)
    EventHistogram.EventHistogramAddDataType(builder, Array.ArrayDouble)
    hist = EventHistogram.EventHistogramEnd(builder)
    builder.Finish(hist)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return buff


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
