import re
import numbers


def parse_time(time) -> float:
    """Parses human-readable timestamp to ns.

    parse_time("130 ms") -> 130000000.0
    parse_time("1.45 s") -> 1450000000.0
    parse_time("12345") -> 12345

    Returns:
        float: Timestamp in nanoseconds.
    """
    if time is None:
        return None

    if isinstance(time, numbers.Number):
        return time

    if type(time) == list:
        return [parse_time(t) for t in time]

    if type(time) == slice:
        return slice(
            parse_time(time.start) if time.start else -float("inf"),
            parse_time(time.stop) if time.stop else float("inf"),
        )

    if time.isnumeric():
        return float(time)

    n = 0

    d = re.search(r"(\d+\.?\d*)\s?d\b", time)
    hr = re.search(r"(\d+\.?\d*)\s?hr\b", time)
    m = re.search(r"(\d+\.?\d*)\s?m\b", time)
    s = re.search(r"(\d+\.?\d*)\s?s\b", time)
    ms = re.search(r"(\d+\.?\d*)\s?ms\b", time)
    us = re.search(r"(\d+\.?\d*)\s?us\b", time)
    ns = re.search(r"(\d+\.?\d*)\s?ns\b", time)

    if d:
        n += float(d.group(1)) * 1e9 * 3600 * 24
    if hr:
        n += float(hr.group(1)) * 1e9 * 3600
    if m:
        n += float(m.group(1)) * 1e9 * 60
    if s:
        n += float(s.group(1)) * 1e9
    if ms:
        n += float(ms.group(1)) * 1e6
    if us:
        n += float(us.group(1)) * 1e3
    if ns:
        n += float(ns.group(1))

    return n
