from just_bin_it.exceptions import JustBinItException


def check_tof(tof_range, missing, invalid):
    if tof_range is None:
        missing.append("TOF range")  # pragma: no mutate
    elif (
        not isinstance(tof_range, list)
        and not isinstance(tof_range, tuple)
        or len(tof_range) != 2
    ):
        invalid.append("TOF range")  # pragma: no mutate


def check_bins(num_bins, missing, invalid):
    if num_bins is None:
        missing.append("number of bins")  # pragma: no mutate
        return
    check_int(num_bins, "number of bins", invalid)


def check_int(value, field, invalid):
    if not isinstance(value, int):
        invalid.append(field)  # pragma: no mutate
        return
    if value < 1:
        invalid.append(field)  # pragma: no mutate


def check_det_range(det_range, missing, invalid):
    if det_range is None:
        missing.append("Detector range")  # pragma: no mutate
    elif (
        not isinstance(det_range, list)
        and not isinstance(det_range, tuple)
        or len(det_range) != 2
    ):
        invalid.append("Detector range")  # pragma: no mutate


def generate_exception(missing, invalid, hist_type):
    error_msg = ""

    if missing:
        error_msg += (
            f"Missing information for {hist_type} histogram: {', '.join(missing)}"
        )  # pragma: no mutate
    if invalid:
        error_msg += (
            f"Invalid information for {hist_type} histogram:  {', '.join(missing)}"
        )  # pragma: no mutate
    if error_msg:
        raise JustBinItException(error_msg)
