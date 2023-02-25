import numpy as np
from bokeh.plotting import Figure

from pipit.vis._util import format_size, format_time
from pipit.vis.core import comm_matrix


def test_format_time():
    assert format_time(0.123) == "0ns"
    assert format_time(1) == "1ns"
    assert format_time(12345) == "12us"
    assert format_time(12345678) == "12ms"
    assert format_time(12345678910) == "12.346s"


def test_format_size():
    assert format_size(0.123) == "0.12 B"
    assert format_size(1) == "1.00 B"
    assert format_size(12345) == "12.35 kB"
    assert format_size(12345678) == "12.35 MB"
    assert format_size(123456789) == "123.46 MB"
    assert format_size(12345678910) == "12.35 GB"
    assert format_size(123456789101112) == "123.46 TB"
    assert format_size(1234567891011121314) == "1234.57 PB"


def test_comm_matrix():
    class FakeTrace:
        def __init__(self, num_procs, multiplier):
            self.num_procs = num_procs
            self.multiplier = multiplier

        def comm_matrix(self):
            ls = np.linspace(0, 10, self.num_procs)
            xx, yy = np.meshgrid(ls, ls)

            cm = np.sin(xx) * np.cos(yy) * self.multiplier
            return cm

    num_procs = [2, 16, 32, 128, 1024]
    multiplier = [100, 1000000, 1000000000]

    for n in num_procs:
        for m in multiplier:
            trace = FakeTrace(n, m)
            for kind in ["heatmap", "scatterplot"]:
                for mapping in ["linear", "log"]:
                    p = comm_matrix(trace, kind=kind, mapping=mapping)
                    assert isinstance(p, Figure)
                    assert p.width > 0
                    assert p.height > 0
                    assert p.visible
                    assert len(p.renderers) > 0
                    assert p.x_range.start == -0.5
                    assert p.x_range.end == n - 0.5
                    assert p.y_range.start == n - 0.5
                    assert p.y_range.end == -0.5
