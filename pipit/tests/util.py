from pipit.util import format_time, parse_time
import numpy as np

ns = [1.23456789 * 10.0**x for x in np.arange(-2, 17)]
hr = [
    "0.01 ns",
    "0.12 ns",
    "1.23 ns",
    "12.35 ns",
    "123.46 ns",
    "1.23 us",
    "12.35 us",
    "123.46 us",
    "1.23 ms",
    "12.35 ms",
    "123.46 ms",
    "1.23 s",
    "12.35 s",
    "123.46 s",
    "20m 34s",
    "3hr 25m",
    "34hr 17m",
    "14d 6hr",
    "142d 21hr",
]


def test_format_time():
    for i in range(0, len(ns)):
        assert format_time(ns[i]) == hr[i]


def test_parse_time():
    for i in range(0, len(ns)):
        np.testing.assert_approx_equal(parse_time(hr[i]), ns[i], 1)
