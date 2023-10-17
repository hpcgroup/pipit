import re
import numbers


def parse_time(ts) -> float:
    """Parses human-readable timestamp to ns.

    parse_time("130 ms") -> 130000000.0
    parse_time("1.45 s") -> 1450000000.0
    parse_time("12345") -> 12345

    Returns:
        float: Timestamp in nanoseconds.
    """
    # If ts is a list or slice, parse each element
    if type(ts) == list:
        return [parse_time(t) for t in ts]

    if type(ts) == slice:
        return slice(
            parse_time(ts.start) if ts.start else -float("inf"),
            parse_time(ts.stop) if ts.stop else float("inf"),
        )

    # If ts is already a number, return it
    if ts is None or isinstance(ts, numbers.Number):
        return ts

    if ts.isnumeric():
        return float(ts)

    # Parse ts string into nanoseconds
    n = 0

    d = re.search(r"(\d+\.?\d*)\s?d\b", ts)
    hr = re.search(r"(\d+\.?\d*)\s?hr\b", ts)
    m = re.search(r"(\d+\.?\d*)\s?m\b", ts)
    s = re.search(r"(\d+\.?\d*)\s?s\b", ts)
    ms = re.search(r"(\d+\.?\d*)\s?ms\b", ts)
    us = re.search(r"(\d+\.?\d*)\s?us\b", ts)
    ns = re.search(r"(\d+\.?\d*)\s?ns\b", ts)

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
