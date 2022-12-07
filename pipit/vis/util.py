import pandas as pd
import numpy as np

from bokeh.models import (
    PrintfTickFormatter,
    FuncTickFormatter,
    CustomJSHover
)

# Constants

# Custom css for notebook and server
CSS = """
    /* Increase output width */
    .container { width:90% !important; }

    /* Remove tooltip overlap */
    div.bk-tooltip > div.bk > div.bk:not(:last-child) {
        display:none !important;
    }

    /* Change hover cursor */
    div.bk { cursor: default !important; }
    
    /* Tooltip text styling */
    .bk.bk-tooltip-row-label {
        color: black;
        font-weight: bold;
    }
    .bk.bk-tooltip-row-value {
        font-family: monospace;
        padding-left: 3px;
    }
"""

# Default color palette to color-code functions by name
# Based on Chrome Trace Viewer
# https://chromium.googlesource.com/external/trace-viewer/+/bf55211014397cf0ebcd9e7090de1c4f84fc3ac0/tracing/tracing/ui/base/color_scheme.html
DEFAULT_PALETTE = [
    "rgb(138,113,152)",
    "rgb(175,112,133)",
    "rgb(127,135,225)",
    "rgb(93,81,137)",
    "rgb(116,143,119)",
    "rgb(178,214,122)",
    "rgb(87,109,147)",
    "rgb(119,155,95)",
    "rgb(114,180,160)",
    "rgb(132,85,103)",
    "rgb(157,210,150)",
    "rgb(148,94,86)",
    "rgb(164,108,138)",
    "rgb(139,191,150)",
    "rgb(110,99,145)",
    "rgb(80,129,109)",
    "rgb(125,140,149)",
    "rgb(93,124,132)",
    "rgb(140,85,140)",
    "rgb(104,163,162)",
    "rgb(132,141,178)",
    "rgb(131,105,147)",
    "rgb(135,183,98)",
    "rgb(152,134,177)",
    "rgb(141,188,141)",
    "rgb(133,160,210)",
    "rgb(126,186,148)",
    "rgb(112,198,205)",
    "rgb(180,122,195)",
    "rgb(203,144,152)",
]


# Formatters
process_tick_formatter = PrintfTickFormatter(format="Process %d")


def format_time(ns):
    """Converts timestamp/timedelta from ns to something more readable"""

    if ns < 1e3:  # Less than 1us --> ns
        return str(round(ns)) + "ns"
    if ns < 1e6:  # Less than 1ms --> us
        return str(round(ns / 1e3)) + "us"
    if ns < 1e9:  # Less than 1s --> ms
        return str(round(ns / 1e6)) + "ms"
    else:
        return str(round(ns / 1e9, 3)) + "s"


# JS expression equivalent to `format_time` function above; assumes:
# - `x` is the value (in ns) being compared to determine units
# - `y` is the value (in ns) actually being formatted
format_time_js = """
    if(x < 1e3)
        return Math.round(y) + "ns";
    if(x < 1e6)
        return Math.round(y / 1e3) + "us";
    if(x < 1e9)
        return Math.round(y / 1e6) + "ms";
    else
        return (y / 1e9).toFixed(3) + "s";
"""

# Used to format ticks for time-based axes
# https://docs.bokeh.org/en/2.4.1/docs/reference/models/formatters.html#functickformatter
time_tick_formatter = FuncTickFormatter(
    code=f"""
        let x = Math.max(...ticks) - Math.min(...ticks);
        let y = tick;
        {format_time_js}
    """
)

# Used to format tooltips for time-based values
# https://docs.bokeh.org/en/latest/docs/reference/models/tools.html#bokeh.models.CustomJSHover
time_hover_formatter = CustomJSHover(
    code=f"""
        let x = value;
        let y = value;
        {format_time_js}
    """
)


def format_size(b):
    """Converts bytes to something more readable"""

    if b < 1e3:  # Less than 1 KB -> byte
        return f"{b:.2f} bytes"
    if b < 1e6:  # Less than 1 MB -> KB
        return f"{(b / 1e3):.2f} KB"
    if b < 1e9:  # Less than 1 GB -> MB
        return f"{(b / 1e6):.2f} MB"
    if b < 1e12:  # Less than 1 TB -> GB
        return f"{(b / 1e9):.2f} GB"
    if b < 1e15:  # Less than 1 PB -> TB
        return f"{(b / 1e12):.2f} TB"
    else:
        return f"{(b / 1e15):.2f} PB"


# JS expression equivalent to `format_size` function above; assumes:
# - `x` is the value (in bytes) being compared to determine units
# - `y` is the value (in bytes) actually being formatted
format_size_js = """
    if(x < 1e3)
        return (y).toFixed(2) + " bytes";
    if(x < 1e6)
        return (y / 1e3).toFixed(2) + " KB";
    if(x < 1e9)
        return (y / 1e6).toFixed(2) + " MB";
    if(x < 1e12)
        return (y / 1e9).toFixed(2) + " GB";
    if(x < 1e15)
        return (y / 1e12).toFixed(2) + " TB";
    else
        return (y / 1e15).toFixed(2) + " PB";
"""

# Used to format ticks for size-based axes
# https://docs.bokeh.org/en/2.4.1/docs/reference/models/formatters.html#functickformatter
size_tick_formatter = FuncTickFormatter(
    code=f"""
        let x = Math.max(...ticks) - Math.min(...ticks);
        let y = tick;
        {format_size_js}
    """
)

# Used to format tooltips for size-based values
# https://docs.bokeh.org/en/latest/docs/reference/models/tools.html#bokeh.models.CustomJSHover
size_hover_formatter = CustomJSHover(
    code=f"""
        let x = value;
        let y = value;
        {format_size_js}
    """
)


# Other utility functions
def in_notebook():
    """Determines if we are in a notebook environment"""
    try:
        from IPython import get_ipython

        if "IPKernelApp" not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


def clamp(n, smallest, largest):
    """Clamps a value between a min and max bound"""
    return max(smallest, min(n, largest))


def generate_cmap(series, palette):
    """Maps a categorical series to a color palette (list of css colors)"""
    names = series.unique().tolist()

    cmap = {names[i]: palette[i] for i in range(len(names))}
    return cmap


def time_series(T=1, N=100, mu=1, sigma=0.3, S0=20):
    """Generates parameterized noisy time series"""
    dt = float(T) / N
    t = np.linspace(0, T, N)
    W = np.random.standard_normal(size=N)
    W = np.cumsum(W) * np.sqrt(dt)  # standard brownian motion
    X = (mu - 0.5 * sigma**2) * t + sigma * W
    S = S0 * np.exp(X)  # geometric brownian motion
    return S


def fake_time_profile(samples, num_bins, functions):
    """Generates sample time-profile data for vis testing"""
    bins = list(range(0, num_bins))

    bins_sample = np.random.choice(bins, samples)
    function_sample = np.random.choice(functions, samples)
    time = np.random.randint(1, 5, size=samples)

    df = pd.DataFrame({"bin": bins_sample, "function": function_sample, "time": time})

    return df


def get_height(num_ys, max_height=600):
    """Calculate optimal height for plot based on number of elements on the y-axis"""
    return clamp(num_ys * 45, 150, max_height)
