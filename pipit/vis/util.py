import yaml
from bokeh.models import BasicTicker, CustomJSHover, FuncTickFormatter
from bokeh.plotting import output_notebook
from bokeh.plotting import show as bk_show
from bokeh.themes import Theme
from pipit import config


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
        doc.add_root(p)
        doc.theme = Theme(
            json=yaml.load(
                config["theme"],
                Loader=yaml.FullLoader,
            )
        )

    if in_notebook():
        # If notebook, show it in output cell
        output_notebook(hide_banner=True)
        bk_show(bkapp, notebook_url=config["notebook_url"])
    else:
        # If standalone, start HTTP server and show in browser
        from bokeh.server.server import Server

        server = Server({"/": bkapp}, port=0, allow_websocket_origin=["*"])
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()


def get_tooltips(tooltips_dict):
    """Returns nicely formatted HTML tooltips from a dict"""

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


def clamp(value, min_val, max_val):
    """Clamps value to min and max bounds"""

    if value < min_val:
        return min_val
    if value > max_val:
        return max_val
    return value


# Custom tickers and formatters

# JS expression to convert bytes to human-readable string
# "x" is the value (in bytes) being compared
# "y" is the value (in bytes) being formatted
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


def get_process_ticker(N):
    return BasicTicker(
        base=2, desired_num_ticks=min(N, 16), min_interval=1, num_minor_ticks=0
    )


def get_size_hover_formatter():
    return CustomJSHover(
        code=f"""
            let x = value;
            let y = value;
            {JS_FORMAT_SIZE}
        """
    )


def get_size_tick_formatter(ignore_range=False):
    x = "tick" if ignore_range else "Math.max(...ticks) - Math.min(...ticks);"
    return FuncTickFormatter(
        code=f"""
            let x = {x}
            let y = tick;
            {JS_FORMAT_SIZE}
        """
    )


# Color palette

# Default color palette is based on bokeh.palettes.Category20_20
# See https://docs.bokeh.org/en/latest/docs/reference/palettes.html#d3-palettes

DEFAULT_RESERVED = {
    "MPI_Send": "#1f77b4",
    "MPI_Isend": "#1f77b4",
    "MPI_Recv": "#d62728",
    "MPI_Irecv": "#d62728",
    "MPI_Wait": "#c7c7c7",
    "MPI_Waitany": "#c7c7c7",
    "MPI_Waitall": "#c7c7c7",
    "Idle": "#c7c7c7",
}

DEFAULT_LIGHT = [
    "#aec7e8",
    "#ffbb78",
    "#98df8a",
    "#ff9896",
    "#c5b0d5",
    "#c49c94",
    "#f7b6d2",
    "#dbdb8d",
    "#9edae5",
]

DEFAULT_DARK = [
    "#ff7f0e",
    "#2ca02c",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#bcbd22",
    "#17becf",
]


def generate_palette(
    trace,
    reserved=DEFAULT_RESERVED,
    light=DEFAULT_LIGHT,
    dark=DEFAULT_DARK,
):
    """Generates color palette for a trace.

    Assigns light colors for even depths, and dark colors for odd depths to maximize
    contrast in nested functions.

    Returns:
        dict: Dictionary mapping function name to CSS color value.
    """

    # Calculate inc time and depth
    trace.calc_inc_metrics(["Timestamp (ns)"])
    trace._match_caller_callee()

    # Get all function names from trace
    func = trace.events[trace.events["Event Type"] == "Enter"]
    names = reversed(trace.flat_profile(["time.inc"]).index.tolist())

    # Get the depth of each function
    depths = (
        func.groupby("Name")["_depth"]
        .agg(lambda x: x.value_counts().index[0])
        .to_dict()
    )

    # Start with palette being a copy of reserved colors
    palette = reserved.copy()

    # Initialize indices for light and dark colors
    dark_index = 0
    light_index = 0

    # Iterate over function names and assign colors to each
    for i, f in enumerate(names):
        if f not in palette:
            # Assign light color for even-depth, and dark color for odd-depth
            if depths[f] % 2 == 0:
                palette[f] = light[light_index % len(light)]
                light_index += 1
            else:
                palette[f] = dark[dark_index % len(dark)]
                dark_index += 1

    return palette
