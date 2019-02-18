import flatbuffers
import numpy as np
from fbschemas.ev42.EventMessage import EventMessage
import fbschemas.hs00.EventHistogram as EventHistogram
import fbschemas.hs00.DimensionMetaData as DimensionMetaData
import fbschemas.hs00.ArrayDouble as ArrayDouble
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


def deserialise_hs00(buf):
    """
    Convert flatbuffer into a histogram.

    :param buf:
    :return: dict of histogram information
    """
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

        info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": bins.tolist(),
            "type": bin_type,
        }
        dims.append(info)

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError("Type of the data array is incorrect")

    data_fb = event_hist.Data()
    temp = ArrayDouble.ArrayDouble()
    temp.Init(data_fb.Bytes, data_fb.Pos)
    data = temp.ValueAsNumpy()
    shape = event_hist.CurrentShapeAsNumpy().tolist()

    hist = {
        "source": event_hist.Source().decode("utf-8"),
        "shape": shape,
        "dims": dims,
        "data": data.reshape(shape),
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


def serialise_hs00(histogrammer):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogrammer: The histogrammer containing the histogram to serialise.
    :return: The raw buffer of the FlatBuffers message.
    """
    file_identifier = b"hs00"

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

    ArrayDouble.ArrayDoubleStartValueVector(builder, data_len)
    # FlatBuffers builds arrays backwards
    for x in reversed(histogram.flatten()):
        builder.PrependFloat64(x)
    data = builder.EndVector(data_len)
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, data)
    pos_data = ArrayDouble.ArrayDoubleEnd(builder)

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    EventHistogram.EventHistogramAddSource(builder, source)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    EventHistogram.EventHistogramAddData(builder, pos_data)
    EventHistogram.EventHistogramAddDataType(builder, Array.ArrayDouble)
    hist = EventHistogram.EventHistogramEnd(builder)
    builder.Finish(hist)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return buff
