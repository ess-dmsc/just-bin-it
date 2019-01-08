import flatbuffers
from fbschemas.ev42.EventMessage import EventMessage
import fbschemas.hs00.EventHistogram as EventHistogram
import fbschemas.hs00.DimensionMetaData as DimensionMetaData
import fbschemas.hs00.ArrayFloat as ArrayFloat
from fbschemas.hs00.Array import Array


def deserialise_ev42(buf):
    """
    Deserialise an ev42 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A dictionary of the deserialised values.
    """
    event = EventMessage.GetRootAsEventMessage(buf, 0)

    data = {
        "message_id": event.MessageId(),
        "pulse_time": event.PulseTime(),
        "source": event.SourceName().decode("utf-8"),
        "det_ids": event.DetectorIdAsNumpy(),
        "tofs": event.TimeOfFlightAsNumpy(),
    }
    return data


def _serialise_metadata(builder, edges, length):
    ArrayFloat.ArrayFloatStartValueVector(builder, len(edges))
    # FlatBuffers builds arrays backwards
    for x in reversed(edges):
        builder.PrependFloat32(x)
    bins = builder.EndVector(len(edges))
    # Add the bins
    ArrayFloat.ArrayFloatStart(builder)
    ArrayFloat.ArrayFloatAddValue(builder, bins)
    pos_bin = ArrayFloat.ArrayFloatEnd(builder)

    DimensionMetaData.DimensionMetaDataStart(builder)
    DimensionMetaData.DimensionMetaDataAddLength(builder, length)
    DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, pos_bin)
    DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, Array.ArrayFloat)
    return DimensionMetaData.DimensionMetaDataEnd(builder)


def serialise_hs00(histogrammer):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogrammer: The histogrammer containing the histogram to serialise.
    :return: The raw buffer of the FlatBuffers message.
    """
    histogram = histogrammer.histogram
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString("just-bin-it")

    # Build shape array
    rank = len(histogram.shape)
    EventHistogram.EventHistogramStartCurrentShapeVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for s in reversed(histogram.shape):
        builder.PrependUint32(s)
    shape = builder.EndVector(rank)

    # Build dimensions metadata
    metadata = []
    # Build the x bins vector
    metadata = [_serialise_metadata(builder, histogrammer.x_edges, histogram.shape[0])]

    # Build the y bins vector, if present
    if hasattr(histogrammer, "y_edges"):
        metadata.append(
            _serialise_metadata(builder, histogrammer.y_edges, histogram.shape[1])
        )

    EventHistogram.EventHistogramStartDimMetadataVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for m in reversed(metadata):
        builder.PrependUOffsetTRelative(m)
    metadata_vector = builder.EndVector(rank)

    # Build the data
    data_len = len(histogram)
    if len(histogram.shape) == 2:
        # 2-D data will be flattened into one array
        data_len = histogram.shape[0] * histogram.shape[1]

    ArrayFloat.ArrayFloatStartValueVector(builder, data_len)
    # FlatBuffers builds arrays backwards
    for x in reversed(histogram.flatten()):
        builder.PrependFloat32(x)
    data = builder.EndVector(data_len)
    ArrayFloat.ArrayFloatStart(builder)
    ArrayFloat.ArrayFloatAddValue(builder, data)
    pos_data = ArrayFloat.ArrayFloatEnd(builder)

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    EventHistogram.EventHistogramAddSource(builder, source)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    EventHistogram.EventHistogramAddData(builder, pos_data)
    hist = EventHistogram.EventHistogramEnd(builder)
    builder.Finish(hist)
    return builder.Output()
