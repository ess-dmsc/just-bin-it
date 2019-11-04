class JustBinItException(Exception):
    pass


class SourceException(JustBinItException):
    pass


class TooOldTimeRequestedException(JustBinItException):
    pass


class KafkaException(JustBinItException):
    pass
