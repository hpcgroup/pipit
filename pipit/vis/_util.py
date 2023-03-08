# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys

from bokeh.io import output_notebook, show
from bokeh.models import CustomJSHover, FuncTickFormatter, PrintfTickFormatter

import pipit as pp


# Formatters
def getProcessTickFormatter():
    return PrintfTickFormatter(format="Process %d")


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
FORMAT_TIME_JS = """
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
def getTimeTickFormatter():
    return FuncTickFormatter(
        code=f"""
                let x = Math.max(...ticks) - Math.min(...ticks);
                let y = tick;
                {FORMAT_TIME_JS}
            """
    )


# Used to format tooltips for time-based values
def getTimeHoverFormatter():
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
def getSizeTickFormatter():
    return FuncTickFormatter(
        code=f"""
                let x = Math.max(...ticks) - Math.min(...ticks);
                let y = tick;
                {FORMAT_SIZE_JS}
            """
    )


# Used to format tooltips for size-based values
def getSizeHoverFormatter():
    return CustomJSHover(
        code=f"""
                let x = value;
                let y = value;
                {FORMAT_SIZE_JS}
            """,
    )


# Helper functions
def plot(obj, notebook_url=None):
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

    # Wrap the plot in a Bokeh app
    def bkapp(doc):
        doc.add_root(obj)

    # Case 1: running unit tests
    if "pytest" in sys.modules:
        return obj

    # Case 2: Notebook
    if in_notebook():
        # Add CSS to increase cell width
        from IPython.core.display import HTML, display_html

        display_html(HTML("<style>.container { width: 90% !important }</style>"))

        # Get default notebook url if not given
        if notebook_url is None:
            notebook_url = pp.config["vis"]["notebook_url"]

        # Show it in output cell
        output_notebook(hide_banner=True)
        show(bkapp, notebook_url=notebook_url)

    # Case 3: Standalone Python script
    else:
        # Start a Bokeh server
        from bokeh.server.server import Server

        server = Server({"/": bkapp}, port=0, allow_websocket_origin=["*"])
        server.start(start_loop=False)
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
