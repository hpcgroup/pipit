import math

import numpy as np
import pandas as pd
from bokeh.models import (
    BasicTicker,
    ColorBar,
    ColumnDataSource,
    HoverTool,
    LabelSet,
    LinearColorMapper,
    LogColorMapper,
)
from bokeh.palettes import Blues256
from bokeh.plotting import figure

from ._util import (
    format_size,
    getProcessTickFormatter,
    getSizeHoverFormatter,
    getSizeTickFormatter,
    plot,
    get_html_tooltips,
)


def comm_matrix(trace, kind="heatmap", mapping="linear", notebook_url=None, **kwargs):
    """Plots the communication matrix of a Trace.

    Args:
        trace (pipit.Trace): Trace objects whose communication matrix is being plotted.
        kind (str, optional): Type of plot, can be "heatmap" or "scatterplot". Defaults
            to "heatmap".
        mapping (str, optional): How to map colors (for heatmap) or size (for
            scatterplot), can be "linear", "log", or "constant". Defaults to "linear".
        notebook_url (str, optional): The URL of the current Jupyter notebook.
            Defaults to url set in `pp.config["vis"]["notebook_url"]`.
        **kwargs: Remaining keyword arguments passed to `Trace.comm_matrix`.
    """
    # Get communication matrix
    comm_matrix = trace.comm_matrix(**kwargs)

    # Transform matrix into a stacked form, required for labels and scatterplot
    stacked = (
        pd.DataFrame(comm_matrix)
        .stack()
        .reset_index()
        .rename(columns={"level_1": "sender", "level_0": "receiver", 0: "volume"})
    )

    N = comm_matrix.shape[0]

    # Define color mapping
    if mapping == "linear":
        color_mapper = LinearColorMapper(
            palette=list(reversed(Blues256)), low=1, high=np.amax(comm_matrix)
        )
    elif mapping == "log":
        color_mapper = LogColorMapper(
            palette=list(reversed(Blues256)), low=1, high=np.amax(comm_matrix)
        )
    else:
        color_mapper = LinearColorMapper(palette="Viridis256", low=1, high=1)

    # Create Bokeh plot
    p = figure(
        title="Communication Matrix",
        x_axis_label="Sender",
        y_axis_label="Receiver",
        x_range=(-0.5, N - 0.5),
        y_range=(N - 0.5, -0.5),
        x_axis_location="above",
        tools="hover,pan,reset,wheel_zoom,save",
        sizing_mode="stretch_width",
    )

    # Add heatmap, color bar, and labels
    if kind == "heatmap":
        p.image(
            image=[np.flipud(comm_matrix)],
            x=-0.5,
            y=N - 0.5,
            dw=N,
            dh=N,
            color_mapper=color_mapper,
        )

        color_bar = ColorBar(
            color_mapper=color_mapper,
            major_label_text_font_size="9px",
            formatter=getSizeTickFormatter(),
            border_line_color=None,
        )
        p.add_layout(color_bar, "right")

        if N <= 32:
            stacked["color"] = np.where(
                stacked["volume"] > stacked["volume"].max() / 2, "white", "black"
            )
            stacked["volume_formatted"] = stacked["volume"].apply(format_size)
            labels = LabelSet(
                x="sender",
                y="receiver",
                text="volume_formatted",
                source=ColumnDataSource(stacked),
                text_align="center",
                text_font_size="10px",
                text_color="color",
                text_baseline="middle",
                level="overlay",
            )
            p.add_layout(labels)

        # Hide grid for heatmap
        p.xgrid.visible = False
        p.ygrid.visible = False

    # Add circles
    if kind == "scatterplot":
        # Normalize circle size
        stacked["volume_normalized"] = stacked["volume"] / stacked["volume"].max()
        stacked["volume_normalized"] = np.sqrt(stacked["volume_normalized"]) * 20

        # Create a column called "image" so that we can use one set of tooltips
        # for heatmap and scatterplot
        stacked["image"] = stacked["volume"]
        p.circle(
            x="sender",
            y="receiver",
            size="volume_normalized",
            source=ColumnDataSource(stacked),
            alpha=0.6,
        )

    # Additional plot config
    p.xaxis.ticker = BasicTicker(
        base=2, desired_num_ticks=min(N, 32), min_interval=1, num_minor_ticks=0
    )
    p.yaxis.ticker = BasicTicker(
        base=2, desired_num_ticks=min(N, 16), min_interval=1, num_minor_ticks=0
    )
    p.xaxis.major_label_orientation = math.pi / 6 if N > 12 else "horizontal"
    p.xaxis.formatter = getProcessTickFormatter()
    p.yaxis.formatter = getProcessTickFormatter()

    # Configure hover
    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Sender": "Process $x{0.}",
            "Receiver": "Process $y{0.}",
            "Bytes": "@image{custom}",
        }
    )
    hover.formatters = {"@image": getSizeHoverFormatter()}

    # Return plot with wrapper function
    return plot(p, notebook_url=notebook_url)


def message_histogram(trace, bins=20, notebook_url=None, **kwargs):
    """Plots histogram of message frequency by size"""

    # Get histogram
    hist, edges = trace.message_histogram(bins, **kwargs)
    xs = [
        f"{format_size(edges[i])} - {format_size(edges[i+1])}" for i in range(len(hist))
    ]

    # Create bokeh plot
    p = figure(
        title="Message Histogram",
        x_range=xs,
        y_range=(0, np.max(hist) + np.max(hist) / 4),
        x_axis_label="Message size",
        y_axis_label="Count",
        tools="xpan,xwheel_zoom,hover,save,reset",
        sizing_mode="stretch_width",
    )

    p.vbar(x=xs, top=hist, bottom=0, width=0.5, fill_alpha=0.6)

    p.xaxis.major_label_orientation = math.pi / 6 if bins > 12 else "horizontal"
    p.xgrid.visible = False

    return plot(p, notebook_url=notebook_url)
