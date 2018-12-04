from fbschemas.ev42.EventMessage import EventMessage as event_msg


def deserialise_ev42(buf):
    """
    Deserialise an ev42 FlatBuffers message.

    :param buf: The raw buffer of the FlatBuffers message.
    :return: A dictionary of the deserialised values
    """
    event = event_msg.GetRootAsEventMessage(buf, 0)

    data = {
        "message_id": event.MessageId(),
        "pulse_time": event.PulseTime(),
        "source": event.SourceName().decode("utf-8"),
        "det_ids": event.DetectorIdAsNumpy(),
        "tofs": event.TimeOfFlightAsNumpy(),
    }
    return data
