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
from bokeh.palettes import Blues256, Category20_20
from bokeh.plotting import figure

from ._util import (
    format_size,
    format_time,
    getProcessTickFormatter,
    getSizeHoverFormatter,
    getSizeTickFormatter,
    getTimeTickFormatter,
    plot,
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
        tools=[
            "pan,reset,wheel_zoom,save",
            HoverTool(
                tooltips={
                    "Sender": "Process $x{0.}",
                    "Receiver": "Process $y{0.}",
                    "Bytes": "@image{custom}",
                },
                formatters={"@image": getSizeHoverFormatter()},
            ),
        ],
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

    # Return plot with wrapper function
    return plot(p, notebook_url=notebook_url)


def time_profile(trace, notebook_url=None, *args, **kwargs):
    # Get time profile
    bins, times = trace.time_profile(*args, **kwargs)

    # Generate x labels
    xs = [f"{format_time(_bin[0])} - {format_time(_bin[1])}" for _bin in bins]

    # Transform data into expected format for plotting stacked bars
    data = dict(xs=xs)
    functions = trace.events[trace.events["Event Type"] == "Enter"]["Name"].unique()
    for function in functions:
        data[function] = [func.get(function, 0) for func in times]

    # Create Bokeh plot
    p = figure(
        title="Time Profile",
        x_range=xs,
        x_axis_label="Time Bin",
        y_axis_label="Time Spent (Exc)",
        tools="hover,xpan,xwheel_zoom,reset,save",
        sizing_mode="stretch_width",
    )

    # Add stacked bars
    p.vbar_stack(
        functions,
        x="xs",
        width=0.5,
        color=Category20_20[: len(functions)],
        source=data,
        legend_label=functions.tolist(),
        fill_alpha=0.6,
    )

    # Additional plot config
    p.yaxis.formatter = getTimeTickFormatter()
    p.y_range.start = 0
    p.xgrid.grid_line_color = None
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None
    p.xaxis.major_label_orientation = math.pi / 6 if len(bins) > 12 else "horizontal"

    # Move legend to right side
    p.add_layout(p.legend[0], "right")

    # Return plot with wrapper function
    plot(p, notebook_url=notebook_url)
