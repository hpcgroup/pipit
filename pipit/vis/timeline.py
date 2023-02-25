import numpy as np
import pandas as pd
from bokeh.models import FactorRange, RangeTool, Range1d
from bokeh.plotting import figure
from bokeh.transform import factor_cmap, dodge
from bokeh.layouts import column

from ._util import plot, getTimeTickFormatter, format_time
import pipit as pp


def plot_timeline(trace, range_selector=False):
    """Plots summary timeline of events in a Trace."""
    df = trace.events.copy(deep=False).reset_index()

    df["Process"] = "Process " + df["Process"].astype("str")
    df["Depth"] = "Depth " + df["Depth"].astype("int").astype("str")
    df["y"] = df[["Process", "Depth"]].apply(lambda x: (x[0], str(x[1])), axis=1)

    df["mtype"] = "circle"
    # df.loc[df["Name"] == "MpiSend", "mtype"] = "triangle"
    # df.loc[df["Name"] == "MpiRecv", "mtype"] = "inverted_triangle"

    index_cmap = factor_cmap(
        "Name",
        palette=pp.config["vis"]["colors"],
        factors=sorted(df.Name.unique()),
        end=1,
    )

    p = figure(
        output_backend="webgl",
        y_range=FactorRange(*sorted(df["y"].unique(), reverse=True)),
        sizing_mode="stretch_width",
        height=min(400, len(df["y"].unique()) * 80 + 60),
        title="Event Timeline",
        tools=["xpan", "xwheel_zoom", "hover"],
        x_axis_location="above",
    )

    p.hbar(
        y="y",
        left="Timestamp (ns)",
        right="Matching Timestamp",
        height=1,
        source=df[df["Event Type"] == "Enter"],
        fill_color=index_cmap,
        line_color="black",
        line_width=0.5,
        fill_alpha=1,
    )

    sends = df[df["Name"] == "MpiSend"]
    recvs = df[df["Name"] == "MpiRecv"]

    comm = pd.DataFrame()
    comm["x0"] = sends["Timestamp (ns)"].values
    comm["y0"] = sends["y"].values
    comm["x1"] = recvs["Timestamp (ns)"].values
    comm["y1"] = recvs["y"].values
    comm["cx0"] = comm["x0"] + (comm["x1"] - comm["x0"]) * 0.5
    comm["cx1"] = comm["x1"] - (comm["x1"] - comm["x0"]) * 0.5

    p.bezier(
        x0="x0",
        y0=dodge("y0", 0.7, range=p.y_range),
        x1="x1",
        y1=dodge("y1", 0.7, range=p.y_range),
        cx0="cx0",
        cy0=dodge("y0", 0.7, range=p.y_range),
        cx1="cx1",
        cy1=dodge("y1", 0.7, range=p.y_range),
        source=comm,
        line_width=1,
        line_color="black",
        alpha=0.6,
    )

    p.scatter(
        x="Timestamp (ns)",
        y=dodge("y", 0.7, range=p.y_range),
        source=df[df["Event Type"] == "Instant"],
        size=9,
        color="MediumAquaMarine",
        alpha=0.5,
        marker="mtype",
    )

    p.xaxis.formatter = getTimeTickFormatter()
    p.x_range.range_padding = 0.2
    p.y_range.range_padding = 0.5
    p.yaxis.group_label_orientation = 0
    p.yaxis.major_label_text_font_size = "0pt"
    p.ygrid.grid_line_color = None

    if range_selector:
        select = figure(
            title="Drag the middle and edges of the selection box to change the range",
            height=130,
            sizing_mode="stretch_width",
            y_range=p.y_range,
            y_axis_type=None,
            tools="",
            toolbar_location=None,
            background_fill_color="#efefef",
        )

        range_tool = RangeTool(x_range=p.x_range)
        range_tool.overlay.fill_color = "navy"
        range_tool.overlay.fill_alpha = 0.2

        select.hbar(
            y="Process",
            left="Timestamp (ns)",
            right="Matching Timestamp",
            height=2,
            source=df[df["Event Type"] == "Enter"],
            fill_color=index_cmap,
            line_color="black",
            line_width=0.5,
            fill_alpha=1,
        )
        select.xaxis.formatter = getTimeTickFormatter()
        select.ygrid.grid_line_color = None
        select.add_tools(range_tool)
        select.toolbar.active_multi = range_tool

        plot(column(p, select, sizing_mode="stretch_width"))
    else:
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
