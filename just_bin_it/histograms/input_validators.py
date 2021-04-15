from collections.abc import Iterable

from just_bin_it.exceptions import JustBinItException


def is_valid(
    value,
    dtype: type = None,
    length: int = None,
    limits: tuple = None,
    validator: object = None,
) -> bool:
    if dtype:
        if not isinstance(value, dtype):
            return False
        if isinstance(value, Iterable) and not all(
            isinstance(x, int) and x >= 0 for x in value
        ):
            return False
    if length and len(value) != length:
        print(f"length and len(value): {length}, {len(value)}")
        return False
    if limits and not (limits[0] <= value <= limits[1]):
        return False
    if validator and not validator(value):
        return False
    return True


def check_tof(tof_range, missing, invalid):
    if tof_range is None:
        missing.append("TOF range")  # pragma: no mutate
        return
    if not is_valid(tof_range, dtype=Iterable, length=2):
        invalid.append("TOF range")  # pragma: no mutate


def check_bins(num_bins, missing, invalid):
    if num_bins is None:
        missing.append("number of bins")  # pragma: no mutate
        return

    if not (
        is_valid(num_bins, dtype=int) or is_valid(num_bins, dtype=Iterable, length=2,)
    ):
        invalid.append("number of bins")  # pragma: no mutate
        return


def check_int(value, field, invalid):
    if not isinstance(value, int):
        invalid.append(field)  # pragma: no mutate
        return
    if value < 1:
        invalid.append(field)  # pragma: no mutate


def check_det_range(det_range, missing, invalid):
    if det_range is None:
        missing.append("Detector range")  # pragma: no mutate
        return
    if not is_valid(det_range, dtype=Iterable, length=2):
        invalid.append("Detector range")  # pragma: no mutate


def generate_exception(missing, invalid, hist_type):
    error_msg = ""

    if missing:
        error_msg += f"Missing information for {hist_type} histogram: {', '.join(missing)}"  # pragma: no mutate
    if invalid:
        error_msg += f"Invalid information for {hist_type} histogram:  {', '.join(missing)}"  # pragma: no mutate
    if error_msg:
        raise JustBinItException(error_msg)
