import numpy as np
import pandas as pd
from bokeh.models import (
    FactorRange,
    RangeTool,
    Range1d,
    WheelZoomTool,
    ColumnDataSource,
    CDSView,
    GroupFilter,
)
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.layouts import column

from ._util import plot, getTimeTickFormatter, format_time
import pipit as pp


def plot_timeline(trace):
    """Plots an overflow timeline of events in a Trace.

    Args:
        trace (pipit.Trace): Trace object whose events are being plotted.
    """

    # Get all events
    df = trace.events.copy(deep=False)

    # Create "y" column based on process and depth
    # Example value: (Process 1, Depth 3.0)
    df["Depth"] = df["Depth"].astype("float").fillna(-1)
    df["y"] = list(
        zip(
            "Process " + df["Process"].astype("str"),
            "Depth " + df["Depth"].astype("str"),
        )
    )

    # Generate comm data for lines
    sends = df[df["Name"] == "MpiSend"]
    recvs = df[df["Name"] == "MpiRecv"]

    comm = pd.DataFrame()
    comm["x0"] = sends["Timestamp (ns)"].values
    comm["y0"] = sends["y"].values
    comm["x1"] = recvs["Timestamp (ns)"].values
    comm["y1"] = recvs["y"].values
    comm["cx0"] = comm["x0"] + (comm["x1"] - comm["x0"]) * 0.5
    comm["cx1"] = comm["x1"] - (comm["x1"] - comm["x0"]) * 0.5

    # Define data sources
    source = ColumnDataSource(df)
    comm_source = ColumnDataSource(comm)

    # Define function color mapping
    function_cmap = factor_cmap(
        "Name",
        palette=pp.config["vis"]["colors"],
        factors=sorted(df.Name.unique()),
        end=1,
    )

    # Create Bokeh plot
    p = figure(
        output_backend="webgl",
        y_range=FactorRange(*sorted(df["y"].unique(), reverse=True)),
        sizing_mode="stretch_width",
        height=min(400, len(df["y"].unique()) * 40 + 30),
        title="Event Timeline",
        tools=["xpan", "xwheel_zoom", "hover"],
        x_axis_location="above",
    )

    # Add bars for functions
    hbar_view = CDSView(
        source=source, filters=[GroupFilter(column_name="Event Type", group="Enter")]
    )
    p.hbar(
        y="y",
        left="Timestamp (ns)",
        right="Matching Timestamp",
        height=1,
        source=source,
        view=hbar_view,
        fill_color=function_cmap,
        line_color="black",
        line_width=0.5,
        fill_alpha=1,
    )

    # Add points for instant events
    scatter_view = CDSView(
        source=source, filters=[GroupFilter(column_name="Event Type", group="Instant")]
    )
    p.scatter(
        x="Timestamp (ns)",
        y="y",
        source=source,
        view=scatter_view,
        size=9,
        alpha=0.5,
        marker="circle",
    )

    # Add lines to connect MPI sends and receives
    p.bezier(
        x0="x0",
        y0="y0",
        x1="x1",
        y1="y1",
        cx0="cx0",
        cy0="y0",
        cx1="cx1",
        cy1="y1",
        source=comm_source,
        line_width=1,
        line_color="black",
        alpha=0.6,
    )

    # Additional plot config
    zoom = p.select(dict(type=WheelZoomTool))
    p.toolbar.active_scroll = zoom[0]
    p.x_range.range_padding = 0.2
    p.xaxis.formatter = getTimeTickFormatter()
    p.yaxis.group_label_orientation = 0
    p.yaxis.major_label_text_font_size = "0pt"
    p.yaxis.major_tick_line_color = None
    p.yaxis.minor_tick_line_color = None
    p.ygrid.grid_line_color = None

    plot(p)


def plot_range_selector(trace):
    (bins, times) = trace.time_profile(num_bins=1024)

    xs = [(_bin[0]) for _bin in bins]

    functions = trace.events[trace.events["Event Type"] == "Enter"]["Name"].unique()

    for time in times:
        total_time = sum(time.values())
        for k, v in time.items():
            time[k] = v / total_time

        # total_time = sum([ math.exp(x/1000000000) for x in time.values() ])
        # for k, v in time.items():
        #     time[k] = math.exp(v/1000000000) / total_time

    data = dict(xs=xs)

    for function in functions:
        data[function] = np.array([func.get(function, 0) for func in times])

    p = figure(
        title="Drag the middle and edges of the selection box to change the range",
        height=400,
        sizing_mode="stretch_width",
        toolbar_location=None,
        y_axis_type=None,
    )

    p.vbar_stack(
        functions,
        x="xs",
        width=bins[0][1] - bins[0][0],
        color=pp.config["vis"]["colors"][: len(functions)],
        source=data,
        legend_label=functions.tolist(),
    )

    range = Range1d(start=min(xs), end=max(xs))

    range_tool = RangeTool(x_range=range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2

    p.ygrid.grid_line_color = None
    p.y_range.start = 0
    p.xgrid.grid_line_color = None
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None
    p.xaxis.formatter = getTimeTickFormatter()
    p.add_tools(range_tool)
    p.toolbar.active_multi = range_tool
    p.legend.label_text_font_size = "12px"
    p.legend.spacing = 0

    p.add_layout(p.legend[0], "right")

    plot(p)


def plot_time_profile(trace, *args, **kwargs):
    (bins, times) = trace.time_profile(*args, **kwargs)

    xs = [f"{format(_bin[0])} - {format_time(_bin[1])}" for _bin in bins]

    functions = trace.events[trace.events["Event Type"] == "Enter"]["Name"].unique()

    data = dict(xs=xs)

    for function in functions:
        data[function] = [func.get(function, 0) for func in times]

    p = figure(
        x_range=xs,
        height=400,
        title="Time Profile",
        toolbar_location=None,
        sizing_mode="stretch_width",
    )

    p.vbar_stack(
        functions,
        x="xs",
        width=0.9,
        color=pp.config["vis"]["colors"][: len(functions)],
        source=data,
        legend_label=functions.tolist(),
    )

    p.yaxis.formatter = getTimeTickFormatter()
    p.y_range.start = 0
    p.xgrid.grid_line_color = None
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None
    p.xaxis.major_label_orientation = 0.3
    p.legend.label_text_font_size = "12px"
    p.legend.spacing = 0

    p.add_layout(p.legend[0], "right")

    plot(p)
