import time

try:
    from time import time_ns

    def time_in_ns():
        return time_ns()


except ImportError:

    def time_in_ns():
        return int(time.time() * 1_000_000_000)
