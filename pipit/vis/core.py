import numpy as np
from bokeh.models import (
    ColorBar,
    HoverTool,
    LinearColorMapper,
    LogColorMapper,
    NumeralTickFormatter,
)
from bokeh.plotting import figure

from .util import (
    clamp,
    get_process_ticker,
    get_size_hover_formatter,
    get_size_tick_formatter,
    get_tooltips,
    show,
)


def comm_matrix(
    data, output="size", cmap="log", palette="Viridis256", return_fig=False
):
    """Plots the trace's communication matrix.

    Args:
        data (numpy.ndarray): a 2D numpy array of shape (N, N) containing the
            communication matrix between N processes.
        output (str, optional): Specifies whether the matrix contains "size"
            or "count" values. Defaults to "size".
        cmap (str, optional): Specifies the color mapping. Options are "log",
            "linear", and "any". Defaults to "log".
        palette (str, optional): Name of Bokeh color palette to use. Defaults to
            "Viridis256".
        return_fig (bool, optional): Specifies whether to return the Bokeh figure
            object. Defaults to False, which displays the result and returns nothing.

    Returns:
        Bokeh figure object if return_fig, None otherwise
    """
    N = data.shape[0]

    # Define color mapper
    if cmap == "linear":
        color_mapper = LinearColorMapper(palette=palette, low=0, high=np.amax(data))
    elif cmap == "log":
        color_mapper = LogColorMapper(
            palette=palette, low=max(np.amin(data), 1), high=np.amax(data)
        )
    elif cmap == "any":
        color_mapper = LinearColorMapper(palette=palette, low=1, high=1)

    # Create bokeh plot
    p = figure(
        x_axis_label="Receiver",
        y_axis_label="Sender",
        x_range=(-0.5, N - 0.5),
        y_range=(N - 0.5, -0.5),
        x_axis_location="above",
        tools="hover,pan,reset,wheel_zoom,save",
        width=90 + clamp(N * 30, 200, 500),
        height=10 + clamp(N * 30, 200, 500),
        toolbar_location="below",
    )

    # Add glyphs and layouts
    p.image(
        image=[np.flipud(data)],
        x=-0.5,
        y=N - 0.5,
        dw=N,
        dh=N,
        color_mapper=color_mapper,
    )

    color_bar = ColorBar(
        color_mapper=color_mapper,
        formatter=get_size_tick_formatter(ignore_range=cmap == "log")
        if output == "size"
        else NumeralTickFormatter(),
        width=15,
    )
    p.add_layout(color_bar, "right")

    # Customize plot
    p.axis.ticker = get_process_ticker(N=N)
    p.grid.visible = False

    # Configure hover
    hover = p.select(HoverTool)
    hover.tooltips = get_tooltips(
        {
            "Sender": "Process $y{0.}",
            "Receiver": "Process $x{0.}",
            "Bytes": "@image{custom}",
        }
        if output == "size"
        else {
            "Sender": "Process $y{0.}",
            "Receiver": "Process $x{0.}",
            "Count": "@image",
        }
    )
    hover.formatters = {"@image": get_size_hover_formatter()}

    # Return plot
    return show(p, return_fig=return_fig)


def message_histogram(
    data,
    return_fig=False,
):
    """Plots the trace's message size histogram.

    Args:
        data (hist, edges): Histogram and edges
        return_fig (bool, optional): Specifies whether to return the Bokeh figure
            object. Defaults to False, which displays the result and returns nothing.

    Returns:
        Bokeh figure object if return_fig, None otherwise
    """
    hist, edges = data

    # Create bokeh plot
    p = figure(
        x_axis_label="Message size",
        y_axis_label="Number of messages",
        tools="hover,save",
    )
    p.y_range.start = 0

    # Add glyphs and layouts
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:])

    # Customize plot
    p.xaxis.formatter = get_size_tick_formatter()
    p.yaxis.formatter = NumeralTickFormatter()

    # Configure hover
    hover = p.select(HoverTool)
    hover.tooltips = get_tooltips(
        {"Bin": "@left{custom} - @right{custom}", "Number of messages": "@top"}
    )
    hover.formatters = {
        "@left": get_size_hover_formatter(),
        "@right": get_size_hover_formatter(),
    }

    # Return plot
    return show(p, return_fig=return_fig)
