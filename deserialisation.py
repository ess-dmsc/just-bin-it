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


def serialise_hs00(histogrammer):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogrammer: The histogrammer containing the histogram to serialise.
    :return: The raw buffer of the FlatBuffers message.
    """
    histogram = histogrammer.histogram

    builder = flatbuffers.Builder(1024)

    source = builder.CreateString("just-bin-it")

    # Build shape array - FlatBuffers requires the vector backwards
    rank = len(histogram.shape)
    EventHistogram.EventHistogramStartCurrentShapeVector(builder, rank)
    for s in reversed(histogram.shape):
        builder.PrependUint32(s)
    shape = builder.EndVector(rank)

    # Build dimensions metadata
    metadata = []
    # Build the x bins vector
    ArrayFloat.ArrayFloatStartValueVector(builder, len(histogrammer.x_edges))
    for x in reversed(histogrammer.x_edges):
        builder.PrependFloat32(x)
    x_bins = builder.EndVector(len(histogrammer.x_edges))
    # Add the bins
    ArrayFloat.ArrayFloatStart(builder)
    ArrayFloat.ArrayFloatAddValue(builder, x_bins)
    x_pos_bin = ArrayFloat.ArrayFloatEnd(builder)

    DimensionMetaData.DimensionMetaDataStart(builder)
    DimensionMetaData.DimensionMetaDataAddLength(builder, histogram.shape[0])
    DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, x_pos_bin)
    DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, Array.ArrayFloat)
    metadata.append(DimensionMetaData.DimensionMetaDataEnd(builder))

    # Build the y bins vector, if present
    if hasattr(histogrammer, "y_edges"):
        ArrayFloat.ArrayFloatStartValueVector(builder, len(histogrammer.x_edges))
        for y in reversed(histogrammer.y_edges):
            builder.PrependFloat32(y)
        y_bins = builder.EndVector(len(histogrammer.y_edges))
        # Add the bins
        ArrayFloat.ArrayFloatStart(builder)
        ArrayFloat.ArrayFloatAddValue(builder, y_bins)
        y_pos_bin = ArrayFloat.ArrayFloatEnd(builder)

        DimensionMetaData.DimensionMetaDataStart(builder)
        DimensionMetaData.DimensionMetaDataAddLength(builder, histogram.shape[1])
        DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, y_pos_bin)
        DimensionMetaData.DimensionMetaDataAddBinBoundariesType(
            builder, Array.ArrayFloat
        )
        metadata.append(DimensionMetaData.DimensionMetaDataEnd(builder))

    EventHistogram.EventHistogramStartDimMetadataVector(builder, rank)
    for m in reversed(metadata):
        builder.PrependUOffsetTRelative(m)
    metadata_vector = builder.EndVector(rank)

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    EventHistogram.EventHistogramAddSource(builder, source)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    hist = EventHistogram.EventHistogramEnd(builder)
    builder.Finish(hist)
    return builder.Output()
