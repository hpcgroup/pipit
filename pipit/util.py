import re


def format_time(n: float) -> str:
    """Converts timestamp/timedelta from ns to human-readable time"""
    # Adapted from https://github.com/dask/dask/blob/main/dask/utils.py

    if n >= 1e9 * 24 * 60 * 60 * 2:
        d = int(n / 1e9 / 3600 / 24)
        h = int((n / 1e9 - d * 3600 * 24) / 3600)
        return f"{d}d {h}hr"

    if n >= 1e9 * 60 * 60 * 2:
        h = int(n / 1e9 / 3600)
        m = int((n / 1e9 - h * 3600) / 60)
        return f"{h}hr {m}m"

    if n >= 1e9 * 60 * 10:
        m = int(n / 1e9 / 60)
        s = int(n / 1e9 - m * 60)
        return f"{m}m {s}s"

    if n >= 1e9:
        return "%.2f s" % (n / 1e9)

    if n >= 1e6:
        return "%.2f ms" % (n / 1e6)

    if n >= 1e3:
        return "%.2f us" % (n / 1e3)

    return "%.2f ns" % n


def parse_time(time) -> float:
    """Converts human-readable time to ns"""
    if type(time) == float or type(time) == int:
        return time

    if type(time) == list:
        return [parse_time(t) for t in time]

    if type(time) == slice:
        return slice(
            parse_time(time.start) if time.start else -float("inf"),
            parse_time(time.stop) if time.stop else float("inf"),
        )

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
