# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from bokeh.io import output_notebook, show as bk_show
from bokeh.models import (
    CustomJSHover,
    CustomJSTickFormatter,
    PrintfTickFormatter,
    NumeralTickFormatter,
    BasicTicker,
)

# from bokeh.palettes import Category20_20
from bokeh.transform import factor_cmap
from bokeh.themes import Theme
import yaml

import math
import numpy as np
import pandas as pd

import pipit as pp


# Formatters
def get_process_ticker(nranks):
    return BasicTicker(
        base=2, desired_num_ticks=min(nranks, 16), min_interval=1, num_minor_ticks=0
    )


def get_process_tick_formatter():
    return PrintfTickFormatter(format="Process %d")


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


# JS expression equivalent to `format_time` function above; assumes:
# - `x` is the value (in ns) being compared to determine units
# - `y` is the value (in ns) actually being formatted
JS_FORMAT_TIME = """
    if (x >= 1e9 * 24 * 60 * 60 * 2) {
        d = Math.round(y / 1e9 / 3600 / 24)
        h = Math.round((y / 1e9 - d * 3600 * 24) / 3600)
        return `${d}d ${h}hr`
    }

    if (x >= 1e9 * 60 * 60 * 2) {
        h = Math.round(y / 1e9 / 3600)
        m = Math.round((y / 1e9 - h * 3600) / 60)
        return `${h}hr ${m}m`
    }

    if (x >= 1e9 * 60 * 10) {
        m = Math.round(y / 1e9 / 60)
        s = Math.round(y / 1e9 - m * 60)
        return `${m}m ${s}s`
    }

    if (x >= 1e9)
        return (y / 1e9).toFixed(2) + "s"

    if (x >= 1e6)
        return (y / 1e6).toFixed(2) + "ms"

    if (x >= 1e3) {
        var ms = Math.floor(y / 1e6);
        var us = ((y - ms * 1e6) / 1e3);

        var str = "";
        if (ms) str += ms + "ms ";
        if (us) str += Math.round(us) + "us";

        return str;
    }

    var ms = Math.floor(y / 1e6);
    var us = Math.floor((y - ms * 1e6) / 1e3);
    var ns = Math.round(y % 1000);

    var str = "";

    if (ms) str += ms + "ms ";
    if (us) str += us + "us ";
    if (ns) str += ns + "ns";

    return str;
"""


# Used to format ticks for time-based axes
def get_time_tick_formatter():
    return CustomJSTickFormatter(
        code=f"""
                let x = Math.max(...ticks) - Math.min(...ticks);
                let y = tick;
                {JS_FORMAT_TIME}
            """
    )


# Used to format tooltips for time-based values
def get_time_hover_formatter():
    return CustomJSHover(
        code=f"""
                let x = value;
                let y = value;
                {JS_FORMAT_TIME}
            """,
    )


def format_size(b):
    """Converts bytes to something more readable"""

    if b < 1e3:  # Less than 1 kB -> byte
        return f"{b:.2f} B"
    if b < 1e6:  # Less than 1 MB -> kB
        return f"{(b / 1e3):.2f} kB"
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
JS_FORMAT_SIZE = """
    if(x < 1e3)
        return (y).toFixed(2) + " B";
    if(x < 1e6)
        return (y / 1e3).toFixed(2) + " kB";
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
def get_size_tick_formatter(ignore_range=False):
    x = "tick" if ignore_range else "Math.max(...ticks) - Math.min(...ticks);"
    return CustomJSTickFormatter(
        code=f"""
            let x = {x}
            let y = tick;
            {JS_FORMAT_SIZE}
        """
    )


# Used to format tooltips for size-based values
def get_size_hover_formatter():
    return CustomJSHover(
        code=f"""
                let x = value;
                let y = value;
                {JS_FORMAT_SIZE}
            """,
    )


def get_percent_tick_formatter():
    return NumeralTickFormatter(format="0.0%")


def get_percent_hover_formatter():
    return CustomJSHover(
        code="""
            return parseFloat(value * 100).toFixed(2)+"%"
        """
    )


# TODO: maybe do this client side with transform
def trimmed(names: pd.Series) -> pd.Series:
    return np.where(
        names.str.len() < 30, names, names.str[0:20] + "..." + names.str[-5:]
    )


def get_trimmed_tick_formatter():
    return CustomJSTickFormatter(
        code="""
            if (tick.length < 30) {
                return tick;
            } else {
                return tick.substr(0, 20) + "..." +
                tick.substr(tick.length - 5, tick.length);
            }
        """
    )


# Helper functions


def in_notebook():
    """Returns True if we are in notebook environment, False otherwise"""
    try:
        from IPython import get_ipython

        if "IPKernelApp" not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


def show(p, return_fig=False):
    """Used to wrap return values of plotting functions.

    If return_figure is True, then just returns the figure object, otherwise starts a
    Bokeh server containing the figure. If we are in a notebook, displays the
    figure in the output cell, otherwise shows figure in new browser tab.

    See https://docs.bokeh.org/en/latest/docs/user_guide/output/jupyter.html#bokeh-server-applications,   # noqa E501
    https://docs.bokeh.org/en/latest/docs/user_guide/server/library.html.
    """
    if return_fig:
        return p

    # Create a Bokeh app containing the figure
    def bkapp(doc):
        doc.clear()
        doc.add_root(p)
        doc.theme = Theme(
            json=yaml.load(
                pp.get_option("theme"),
                Loader=yaml.FullLoader,
            )
        )

    if in_notebook():
        # If notebook, show it in output cell
        output_notebook(hide_banner=True)
        bk_show(bkapp, notebook_url=pp.get_option("notebook_url"))
    else:
        # If standalone, start HTTP server and show in browser
        from bokeh.server.server import Server

        server = Server({"/": bkapp}, port=0, allow_websocket_origin=["*"])
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()


def clamp(value, min_val, max_val):
    """Clamps value to min and max bounds"""

    if value < min_val:
        return min_val
    if value > max_val:
        return max_val
    return value


def get_html_tooltips(tooltips_dict):
    html = """
        <div class="bk-tooltip-content">
            <div>
                <div style="display: table; border-spacing: 2px;">
    """
    for k, v in tooltips_dict.items():
        html += f"""
            <div style="display: table-row;">
                <div class="bk-tooltip-row-label" style="display: table-cell;">
                {k}:
                </div>
                <div class="bk-tooltip-row-value" style="display: table-cell;">
                    <span data-value="">{v}</span>
                </div>
            </div>
        """
    html += """
        </div></div></div>
        <style>
            div.bk-tooltip > div > div:not(:last-child) {
                display:none !important;
            }
            div.bk-tooltip-content > div > div:not(:last-child) {
                display:none !important;
            }
        </style>

    """
    return html


def factorize_tuples(tuples_list):
    unique_values = sorted(set(tuples_list))
    value_to_index = {value: i for i, value in enumerate(unique_values)}
    codes = [value_to_index[value] for value in tuples_list]
    return codes, list(unique_values)


def hex_to_rgb(hex):
    hex = hex.strip("#")

    r, g, b = int(hex[:2], 16), int(hex[2:4], 16), int(hex[4:], 16)
    return (r, g, b)


def rgb_to_hex(rgb):
    r, g, b = rgb
    return "#%02x%02x%02x" % (int(r), int(g), int(b))


def average_hex(*hex):
    """Averages any number of hex colors, returns result in hex"""
    colors = [hex_to_rgb(h) for h in hex]
    return rgb_to_hex(np.mean(colors, axis=0))


def scale_hex(hex, scale):
    """Multiplies a hex color by a scalar, returns result in hex"""
    if scale < 0 or len(hex) != 7:
        return hex

    r, g, b = hex_to_rgb(hex)

    r = int(clamp(r * scale, 0, 255))
    g = int(clamp(g * scale, 0, 255))
    b = int(clamp(b * scale, 0, 255))

    return rgb_to_hex((r, g, b))


def get_height(num_yticks, height_per_tick=400):
    """Calculates ideal plot height based on number of y ticks"""
    return clamp(int(math.log10(num_yticks) * height_per_tick + 50), 200, 900)


LIGHT = [
    "#aec7e8",
    "#ffbb78",
    "#98df8a",
    "#ff9896",
    "#c5b0d5",
    "#c49c94",
    "#f7b6d2",
    # "#c7c7c7",            # reserved for idle
    "#dbdb8d",
    "#9edae5",
]
DARK = [
    # "#1f77b4",            # reserved for send
    "#ff7f0e",
    "#2ca02c",
    # "#d62728",            # reserved for recv
    "#9467bd",
    "#8c564b",
    "#e377c2",
    # "#7f7f7f",            # take out
    "#bcbd22",
    "#17becf",
]


def get_palette(trace, scale=None):
    funcs = trace.events[trace.events["Event Type"] == "Enter"]
    # names = funcs["Name"].unique().tolist()
    names = reversed(trace.flat_profile(["time.inc"]).index.tolist())

    depths = (
        funcs.groupby("Name", observed=True)["_depth"]
        .agg(lambda x: x.value_counts().index[0])
        .to_dict()
    )

    palette = {}

    palette["MPI_Send"] = "#1f77b4"
    palette["MPI_Isend"] = "#1f77b4"

    palette["MPI_Recv"] = "#d62728"
    palette["MPI_Irecv"] = "#d62728"

    palette["MPI_Wait"] = "#c7c7c7"
    palette["MPI_Waitany"] = "#c7c7c7"
    palette["MPI_Waitall"] = "#c7c7c7"
    palette["Idle"] = "#c7c7c7"

    dark_index = 0
    light_index = 0

    for i, f in enumerate(names):
        if f not in palette:
            if depths[f] % 2 == 0:
                # palette[f] = LIGHT[hash(f) % len(LIGHT)]
                palette[f] = LIGHT[light_index % len(LIGHT)]
                light_index += 1
            else:
                # palette[f] = DARK[hash(f) % len(DARK)]
                palette[f] = DARK[dark_index % len(DARK)]
                dark_index += 1

    # apply multiplier
    if scale:
        for k, v in palette.items():
            palette[k] = scale_hex(v, scale)

    return palette


def get_factor_cmap(field_name, trace, **kwargs):
    palette = get_palette(trace, **kwargs)
    return factor_cmap(field_name, list(palette.values()), list(palette.keys()))
