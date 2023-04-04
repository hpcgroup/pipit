# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys

from bokeh.io import output_notebook, show
from bokeh.models import (
    CustomJSHover,
    FuncTickFormatter,
    PrintfTickFormatter,
    NumeralTickFormatter,
)
from bokeh.transform import factor_cmap
from bokeh.themes import Theme
import yaml

import math
import numpy as np
import pandas as pd

# Constants
theme = "default"
notebook_url = "http://localhost:8888"

THEME_DEFAULT = """
    attrs:
        Plot:
            height: 400
            width: 800
            toolbar_location: "above"
            outline_line_width: 0
        Axis:
            axis_label_text_font_style: "bold"
        Toolbar:
            autohide: true
        HoverTool:
            point_policy: "follow_mouse"
        Legend:
            label_text_font_size: "8.5pt"
            spacing: -1
            padding: 0
            border_line_color: null
        Scatter:
            size: 9
"""
THEME_PAPER = """
    attrs:
        Plot:
            height: 420
            width: 800
            toolbar_location: "above"
            outline_line_width: 0
        Title:
            text_font_size: "0pt"
            text_font: "Gill Sans"
        Axis:
            axis_label_text_font_style: "bold"
            axis_label_text_font_size: "18pt"
            axis_label_text_font: "Gill Sans"
            major_label_text_font_size: "16pt"
            major_label_text_font: "Gill Sans"
        ColorBar:
            major_label_text_font_size: "16pt"
            major_label_text_font: "Gill Sans"
        Toolbar:
            autohide: true
        HoverTool:
            point_policy: "follow_mouse"
        Legend:
            label_text_font_size: "15pt"
            label_text_font: "Gill Sans"
            spacing: -1
            padding: 0
            border_line_color: null
        Scatter:
            size: 12
"""


# Formatters
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
FORMAT_TIME_JS = """
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
        return (y / 1e9).toFixed(2) + " s"

    if (x >= 1e6)
        return (y / 1e6).toFixed(2) + " ms"

    if (x >= 1e3)
        return (y / 1e3).toFixed(2) + " us"

    return x.toFixed(2) + " ns"
"""


# Used to format ticks for time-based axes
def get_time_tick_formatter():
    return FuncTickFormatter(
        code=f"""
                let x = Math.max(...ticks) - Math.min(...ticks);
                let y = tick;
                {FORMAT_TIME_JS}
            """
    )


# Used to format tooltips for time-based values
def get_time_hover_formatter():
    return CustomJSHover(
        code=f"""
                let x = value;
                let y = value;
                {FORMAT_TIME_JS}
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
FORMAT_SIZE_JS = """
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
def get_size_tick_formatter():
    return FuncTickFormatter(
        code=f"""
                let x = Math.max(...ticks) - Math.min(...ticks);
                let y = tick;
                {FORMAT_SIZE_JS}
            """
    )


# Used to format tooltips for size-based values
def get_size_hover_formatter():
    return CustomJSHover(
        code=f"""
                let x = value;
                let y = value;
                {FORMAT_SIZE_JS}
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
    return FuncTickFormatter(
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
def plot(obj):
    """Internal function used to wrap return values from plotting functions. If we are in a
        notebook, then `bokeh.io.show` is invoked and the plot is displayed immediately
        in the associated output cell. If we are in the Python shell, then a new Bokeh
        server instance is launched, and the plot is displayed in a new browser tab.
        Both scenarios allow for bidirectional communication between the JS frontend
        and the Python backend.

        See https://docs.bokeh.org/en/latest/docs/user_guide/output/jupyter.html#bokeh-server-applications,   # noqa E501
        https://docs.bokeh.org/en/latest/docs/user_guide/server/library.html.

    Args:
        obj: The Bokeh object to display.
    """
    # if in_notebook():
    #     from IPython.display import display, HTML

    #     display(HTML("<style>.container { width:100% !important; }</style>"))

    # Wrap the plot in a Bokeh app
    def bkapp(doc):
        doc.add_root(obj)
        doc.theme = Theme(
            json=yaml.load(
                THEME_DEFAULT if theme == "default" else THEME_PAPER,
                Loader=yaml.FullLoader,
            )
        )

    # Case 1: running unit tests
    if "pytest" in sys.modules:
        return obj

    # Case 2: Notebook
    if in_notebook():
        # Add CSS to increase cell width
        from IPython.core.display import HTML, display_html

        display_html(
            HTML(
                """
                <style>
                @import url('https://fonts.cdnfonts.com/css/gill-sans?styles=17575');
                </style>
                """
            )
        )

        # Show it in output cell
        output_notebook(hide_banner=True)
        show(bkapp, notebook_url=notebook_url)

    # Case 3: Standalone Python script
    else:
        # Start a Bokeh server
        from bokeh.server.server import Server

        server = Server({"/": bkapp}, port=0, allow_websocket_origin=["*"])
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()


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


def get_html_tooltips(tooltips_dict):
    html = ""
    for k, v in tooltips_dict.items():
        html += f"""
            <div>
                <span style=\"font-size: 12px; font-weight: bold;\">{k}:</span>&nbsp;
                <span style=\"font-size: 12px; font-family: monospace;\">{v}</span>
            </div>
        """
    html += """
        <style>
            div.bk-tooltip > div > div:not(:last-child) {
                display:none !important;
            }
        </style>
    """
    return html


def clamp(val, minimum, maximum):
    """Clamps value to minimum and maximum bounds"""
    if val < minimum:
        return minimum
    if val > maximum:
        return maximum
    return val


# Generated with https://medialab.github.io/iwanthue/
# Slightly more lenient chroma/lightness than default preset
# Hues restrained to "cool"
COOL = [
    "#4d8464",
    "#3e69b2",
    "#7fbe37",
    "#6899ce",
    "#4ac36b",
    "#56b5c7",
    "#52832f",
    "#72c19b",
    "#365525",
    "#97ba63",
]

# Hues restrained to "warm"
WARM = [
    "#ce98d7",
    "#db5036",
    "#9859ca",
    "#a94937",
    "#c849af",
    "#dc8e76",
    "#89578d",
    "#d03c73",
    "#975156",
    "#df809c",
]


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


def get_palette(trace, scale=None):
    # Idea: use cool for MPI, divide warm among non-MPIs
    functions = (
        trace.events[trace.events["Event Type"] == "Enter"]["Name"].unique().tolist()
    )

    mpi = sorted(list(filter(lambda x: x.startswith("MPI_"), functions)))
    nonmpi = sorted(list(set(functions).difference(mpi)))

    palette = {}
    cool = COOL.copy()
    warm = WARM.copy()

    # reserved colors
    palette["MPI"] = average_hex(*cool)

    palette["MPI_Init"] = cool.pop(len(cool) - 1)
    palette["MPI_Finalize"] = palette["MPI_Init"]

    palette["MPI_Send"] = cool.pop(0)
    palette["MPI_Isend"] = palette["MPI_Send"]

    palette["MPI_Recv"] = cool.pop(0)
    palette["MPI_Irecv"] = palette["MPI_Recv"]

    # assign remaining MPI functions based on index
    for i, f in enumerate(mpi):
        if f not in palette:
            palette[f] = cool[i % len(cool)]

    # assign remaining functions based on index
    for i, f in enumerate(nonmpi):
        if f not in palette:
            palette[f] = warm[i % len(warm)]

    # apply multiplier
    if scale:
        for k, v in palette.items():
            palette[k] = scale_hex(v, scale)

    return palette


def get_factor_cmap(field_name, trace, **kwargs):
    palette = get_palette(trace, **kwargs)
    return factor_cmap(field_name, list(palette.values()), list(palette.keys()))
