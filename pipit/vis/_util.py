# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np

import pipit as pp
import random

from bokeh.models import PrintfTickFormatter, FuncTickFormatter, CustomJSHover

# Constants

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
def reload_vis():
    pp.config["vis"]["initialized"] = False
    init_vis()


def init_vis():
    if pp.config["vis"]["initialized"]:
        return

    print("reloading vis")

    import holoviews as hv
    from holoviews import opts

    # Set bokeh to be our backend, and apply css
    hv.extension("bokeh", logo=False, css=pp.config["vis"]["css"])

    # Set bokeh theme
    # hv.renderer("bokeh").theme = pp.config["vis"]["theme"] + "_minimal"

    # Load default colormap
    if pp.config["vis"]["shuffle_colors"]:
        random.shuffle(pp.config["vis"]["colors"])

    # Default hook
    def plot_hook(p, _):
        p.state.toolbar_location = "above"
        p.state.ygrid.visible = False
        if p.state.legend:
            p.state.legend.label_text_font_size = "8pt"
            p.state.legend.spacing = 0
            p.state.legend.location = "top"

    # Apply default opts
    default_opts = dict(
        fontsize={"title": 10, "legend": 8},
        hooks=[plot_hook],
        responsive=True,
        height=300,
    )

    opts.defaults(
        opts.Area(**default_opts, color=pp.config["vis"]["default_color"]),
        opts.Bars(**default_opts, color=pp.config["vis"]["default_color"]),
        opts.Bivariate(**default_opts),
        opts.BoxWhisker(**default_opts),
        opts.Chord(**default_opts),
        opts.Contours(**default_opts),
        opts.Curve(**default_opts, color=pp.config["vis"]["default_color"]),
        opts.Distribution(**default_opts),
        opts.Graph(**default_opts),
        opts.Histogram(**default_opts, color=pp.config["vis"]["default_color"]),
        opts.Image(**default_opts),
        opts.Labels(**default_opts),
        opts.Points(**default_opts),
        opts.Polygons(**default_opts),
        opts.Rectangles(**default_opts),
        opts.Sankey(**default_opts),
        opts.Segments(**default_opts),
    )

    pp.config["vis"]["launch_server"] = not in_notebook()
    pp.config["vis"]["initialized"] = True


def plot(element):
    """Used to wrap return values in vis functions. Launches HTTP server if config
    variable is set to True, else returns the element.

    Args:
        element (hv.Element): HoloViews element to launch in server or return
    """
    if pp.config["vis"]["launch_server"]:
        import panel as pn

        pn.extension(raw_css=pp.config["vis"]["css"])
        pn.panel(element).show()
        return

    return element


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


def get_height(num_ys, max_height=600):
    """Calculate optimal height for plot based on number of elements on the y-axis"""
    return clamp(num_ys * 45, 150, max_height)
