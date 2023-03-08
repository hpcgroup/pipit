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
    WheelZoomTool,
    Grid,
    FixedTicker,
)
from bokeh.palettes import Blues256, Category20_20
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.events import RangesUpdate

from ._util import (
    format_size,
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
            # color={"field": "image", "transform": color_mapper},
        )

    # Additional plot config
    p.xaxis.ticker = BasicTicker(
        base=2, desired_num_ticks=32, min_interval=1, num_minor_ticks=0
    )
    p.yaxis.ticker = BasicTicker(
        base=2, desired_num_ticks=16, min_interval=1, num_minor_ticks=0
    )
    p.xaxis.major_label_orientation = math.pi / 6
    p.xgrid.visible = False
    p.ygrid.visible = False
    p.xaxis.formatter = getProcessTickFormatter()
    p.yaxis.formatter = getProcessTickFormatter()

    # Return plot with wrapper function
    return plot(p, notebook_url=notebook_url)


def timeline(trace, notebook_url=None):
    trace.calc_inc_time()
    trace._match_events()
    trace._match_caller_callee()

    # Number of ranks
    N = int(trace.events["Process"].astype("float").max() + 1)
    min_ts = trace.events["Timestamp (ns)"].min()
    max_ts = trace.events["Timestamp (ns)"].max()

    # Prepare data for plotting
    func = trace.events[trace.events["Event Type"] == "Enter"].copy(deep=False)
    func = func.sort_values(by="time.inc", ascending=False)
    func["y"] = func["Process"].astype("int")
    func["Timestamp (ns)"] = func["Timestamp (ns)"].astype("float32")
    func["_matching_timestamp"] = func["_matching_timestamp"].astype("float32")
    func["time.inc"] = func["time.inc"].astype("float32")
    func = func[["Timestamp (ns)", "_matching_timestamp", "y", "Name"]]

    # Define data source for glyphs
    source = ColumnDataSource(func.head(0))

    # Callback function that updates Bokeh data source
    def update_data_source(event):
        nonlocal source
        x0 = event.x0 if event is not None else min_ts
        x1 = event.x1 if event is not None else max_ts

        # Remove events that are out of bounds
        in_bounds = func[
            ~((func["_matching_timestamp"] < x0) | (func["Timestamp (ns)"] > x1))
        ]

        # Get 500 largest functions
        large = in_bounds.head(5000)
        # small = in_bounds.tail(len(in_bounds) - 500)

        source.data = large

    # Create Bokeh plot
    p = figure(
        title="Timeline",
        x_range=(min_ts, max_ts),
        y_range=(max(15.5, N - 0.5), -0.5),
        x_axis_location="above",
        tools="hover,xpan,reset,xwheel_zoom,save",
        output_backend="webgl",
    )

    # Create color map
    cmap = factor_cmap(
        "Name", palette=Category20_20, factors=sorted(func["Name"].unique()), end=1
    )

    # Add bars for functions
    p.hbar(
        left="Timestamp (ns)",
        right="_matching_timestamp",
        y="y",
        height=1,
        source=source,
        fill_color=cmap,
        line_color="black",
        line_width=0.5,
    )

    # Add custom grid for y-axis
    p.ygrid.visible = False
    g = Grid(
        dimension=1,
        band_fill_color="gray",
        band_fill_alpha=0.1,
        ticker=FixedTicker(ticks=list(np.arange(-1000, 1000) + 0.5)),
    )
    p.add_layout(g)

    # Additional plot config
    p.yaxis.ticker = BasicTicker(
        base=2, desired_num_ticks=16, min_interval=1, num_minor_ticks=0
    )
    p.xaxis.formatter = getTimeTickFormatter()
    p.yaxis.formatter = getProcessTickFormatter()
    p.toolbar.active_scroll = p.select(dict(type=WheelZoomTool))[0]
    p.on_event(RangesUpdate, update_data_source)

    # Make initial call to our callback
    update_data_source(None)

    return plot(p, notebook_url=notebook_url)
