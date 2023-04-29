import yaml
from bokeh.models import BasicTicker, CustomJSHover, FuncTickFormatter
from bokeh.plotting import output_notebook
from bokeh.plotting import show as bk_show
from bokeh.themes import Theme

from .theme import themes

# Constants
notebook_url = "http://localhost:8888"
theme = "default"

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


# Utility functions
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

    # Load Gill Sans font from CDN
    if in_notebook() and theme == "paper":
        from IPython.display import HTML, display_html

        display_html(
            HTML(
                """
                <style>
                @import url('https://fonts.cdnfonts.com/css/gill-sans?styles=17575');
                </style>
                """
            )
        )

    # Create a Bokeh app containing the figure
    def bkapp(doc):
        doc.add_root(p)
        doc.theme = Theme(
            json=yaml.load(
                themes[theme],
                Loader=yaml.FullLoader,
            )
        )

    if in_notebook():
        # If notebook, show it in output cell
        output_notebook(hide_banner=True)
        bk_show(bkapp, notebook_url=notebook_url)
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
