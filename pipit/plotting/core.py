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
    FuncTickFormatter,
    Arrow,
    OpenHead,
    CustomJS,
    BasicTickFormatter,
    NumeralTickFormatter,
)
from bokeh.palettes import RdYlBu11
from bokeh.plotting import figure
from bokeh.transform import dodge
from bokeh.events import RangesUpdate, Tap

from ._util import (
    get_size_hover_formatter,
    get_size_tick_formatter,
    get_time_tick_formatter,
    get_time_hover_formatter,
    get_percent_tick_formatter,
    get_percent_hover_formatter,
    get_trimmed_tick_formatter,
    plot,
    get_html_tooltips,
    get_height,
    get_factor_cmap,
    get_palette,
    clamp,
    trimmed,
)

import datashader as ds


def timeline(trace, show_depth=False, instant_events=False):
    """Displays the events of a trace against time

    Instant events are represented by points, functions are represented by horizontal
    bars, and MPI messages are represented by lines connecting send/receive events."""

    # Generate necessary metrics
    trace.calc_exc_metrics(["Timestamp (ns)"])
    trace._match_events()
    trace._match_caller_callee()

    min_ts = trace.events["Timestamp (ns)"].min()
    max_ts = trace.events["Timestamp (ns)"].max()

    # Prepare data for plotting
    events = (
        trace.events[trace.events["Event Type"].isin(["Enter", "Instant"])]
        .sort_values(by="time.inc", ascending=False)
        .copy(deep=False)
    )
    events["_depth"] = events["_depth"].astype(float).fillna("")

    # Determine y-coordinates from process and depth
    y_tuples = (
        list(zip(events["Process"], events["_depth"]))
        if show_depth
        else list(zip(events["Process"]))
    )

    codes, uniques = pd.factorize(y_tuples, sort=True)
    events["y"] = codes
    num_ys = len(uniques)

    depth_ticks = np.arange(0, num_ys)
    process_ticks = np.array(
        [i for i, v in enumerate(uniques) if len(v) == 1 or v[1] == 0]
    )

    events["name_trimmed"] = trimmed(events["Name"])

    # Only select a subset of columns for plotting
    events = events[
        [
            "Timestamp (ns)",
            "_matching_timestamp",
            "y",
            "Name",
            "time.inc",
            "Process",
            "time.exc",
            "name_trimmed",
            "Event Type",
        ]
    ]

    # Define CDS for glyphs to be empty
    hbar_source = ColumnDataSource(events.head(0))
    scatter_source = ColumnDataSource(events.head(0))
    image_source = ColumnDataSource(
        data=dict(
            image=[np.zeros((50, 16), dtype=np.uint32)], x=[0], y=[0], dw=[0], dh=[0]
        )
    )

    def tap_callback(event):
        x = event.x
        y = event.y

        candidates = events[
            (events["Event Type"] == "Instant")
            & (events["Name"].isin(["MpiSend", "MpiRecv", "MpiIsend", "MpiIrecv"]))
            & (events["y"] == round(y))
        ]

        dx = candidates["Timestamp (ns)"] - x
        distance = pd.Series(dx * dx)

        selected = candidates.iloc[distance.argsort().values]

        if len(selected) >= 1:
            selected = selected.iloc[0]

            match = trace._get_matching_p2p_event(selected.name)
            send = (
                selected
                if selected["Name"] in ["MpiSend", "MpiIsend"]
                else events.loc[match]
            )
            recv = (
                selected
                if selected["Name"] in ["MpiRecv", "MpiIrecv"]
                else events.loc[match]
            )

            arrow = Arrow(
                end=OpenHead(line_color="#28282B", line_width=1.5, size=8),
                line_color="#28282B",
                line_width=1.5,
                x_start=send["Timestamp (ns)"],
                y_start=send["y"] - 0.2 if show_depth else send["y"],
                x_end=recv["Timestamp (ns)"],
                y_end=recv["y"] - 0.2 if show_depth else recv["y"],
                level="overlay",
            )
            p.add_layout(arrow)

    # Callback function that updates CDS
    def update_cds(event):
        x0 = event.x0 if event is not None else min_ts
        x1 = event.x1 if event is not None else max_ts

        # Remove events that are out of bounds
        in_bounds = events[
            (
                (events["Event Type"] == "Instant")
                & (events["Timestamp (ns)"] > x0)
                & (events["Timestamp (ns)"] < x1)
            )
            | (
                (events["Event Type"] == "Enter")
                & (events["_matching_timestamp"] > x0)
                & (events["Timestamp (ns)"] < x1)
            )
        ].copy(deep=False)

        # Update hbar_source to keep 5000 largest functions
        func = in_bounds[in_bounds["Event Type"] == "Enter"]
        large = func.head(5000)
        hbar_source.data = large

        # Update scatter_source to keep sampled events
        if instant_events:
            inst = in_bounds[in_bounds["Event Type"] == "Instant"].copy(deep=False)

            if len(inst) > 500:
                inst["bin"] = pd.cut(x=inst["Timestamp (ns)"], bins=1000, labels=False)

                grouped = inst.groupby(["bin", "y"])
                samples = grouped.first().reset_index()
                samples = samples[~samples["Timestamp (ns)"].isna()]

                scatter_source.data = samples
            else:
                scatter_source.data = inst

        # Rasterize the rest
        small = func.tail(len(func) - 5000).copy(deep=True)
        small["Name"] = small["Name"].astype("str")
        small["Name"] = small["Name"].astype("category")

        if len(small):
            # Create a new Datashader canvas based on plot properties
            cvs = ds.Canvas(
                plot_width=1920 if p.inner_width == 0 else p.inner_width,
                plot_height=num_ys,
                x_range=(x0, x1),
                y_range=(-0.5, num_ys - 0.5),
            )

            # Feed the data into datashader
            agg = cvs.points(small, x="Timestamp (ns)", y="y", agg=ds.count_cat("Name"))

            # Generate image
            img = ds.tf.shade(
                agg,
                color_key=get_palette(trace, 0.7),
            )

            # Update CDS
            image_source.data = dict(
                image=[np.flipud(img.to_numpy())],
                x=[x0],
                y=[num_ys - 0.5],
                dw=[x1 - x0],
                dh=[num_ys],
            )
        else:
            image_source.data = dict(
                image=[np.zeros((50, 16), dtype=np.uint32)],
                x=[x0],
                y=[num_ys - 0.5],
                dw=[x1 - x0],
                dh=[num_ys],
            )

    # Create Bokeh plot
    min_height = 50 + 22 * len(events["Name"].unique())
    plot_height = 80 + 25 * num_ys
    height = clamp(plot_height, min_height, 900)

    p = figure(
        # title="Timeline",
        x_range=(min_ts, max_ts),
        y_range=(num_ys - 0.5, -0.5),
        x_axis_location="above",
        tools="hover,xpan,reset,xbox_zoom,xwheel_zoom,save",
        output_backend="webgl",
        sizing_mode="stretch_width",
        height=height,
        x_axis_label="Time",
    )

    p.min_border_bottom = height - plot_height

    # Create color maps
    fill_cmap = get_factor_cmap("Name", trace)
    line_cmap = get_factor_cmap("Name", trace, scale=0.7)

    # Add lines for each process
    # p.segment(
    #     x0=[0] * len(process_ticks),
    #     x1=[max_ts] * len(process_ticks),
    #     y0=process_ticks,
    #     y1=process_ticks,
    #     line_dash="dotted",
    #     line_color="gray",
    # )

    # Add bars for large functions
    hbar = p.hbar(
        left="Timestamp (ns)",
        right="_matching_timestamp",
        y="y",
        height=1 if show_depth else 0.7,
        source=hbar_source,
        fill_color=fill_cmap,
        line_color=line_cmap,
        line_width=1,
        line_alpha=0.5,
        legend_field="name_trimmed",
    )

    # Add raster for small functions
    p.image_rgba(source=image_source)

    if instant_events:
        scatter = p.scatter(
            x="Timestamp (ns)",
            y=dodge("y", -0.2 if show_depth else 0),
            # size=9,
            line_color="#0868ac",
            alpha=1,
            color="#ccebc5",
            line_width=0.8,
            marker="diamond",
            source=scatter_source,
            legend_label="Instant event",
        )

    # Add custom grid lines
    p.xgrid.visible = False
    p.ygrid.visible = False

    g1 = Grid(
        dimension=1,
        grid_line_color="white",
        grid_line_width=2 if show_depth else 5,
        ticker=FixedTicker(
            ticks=np.concatenate([depth_ticks - 0.49, depth_ticks + 0.49])
        ),
        level="glyph",
    )
    g2 = Grid(
        dimension=1,
        grid_line_width=1,
        # band_fill_color="gray",
        # band_fill_alpha=0.1,
        ticker=FixedTicker(ticks=process_ticks - 0.5),
        level="glyph",
    )
    p.add_layout(g1)
    p.add_layout(g2)

    # Additional plot config
    p.xaxis.formatter = get_time_tick_formatter()
    p.xaxis.minor_tick_line_color = None
    p.yaxis.formatter = FuncTickFormatter(
        args={
            "uniques": uniques,
        },
        code="""
            return "Process " + uniques[Math.floor(tick)][0];
        """,
    )

    p.yaxis.ticker = FixedTicker(ticks=process_ticks + 0.1)
    p.yaxis.major_tick_line_color = None
    p.yaxis.minor_tick_line_color = None

    p.toolbar.active_scroll = p.select(dict(type=WheelZoomTool))[0]
    p.on_event(RangesUpdate, update_cds)
    p.on_event(Tap, tap_callback)

    # Move legend to the right
    # p.legend.location = (10, 35)
    p.add_layout(p.legend[0], "right")

    # Make initial call to our callback
    update_cds(None)

    # Hover config
    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Name": "@Name",
            "Process": "@Process",
            "Enter": "@{Timestamp (ns)}{custom}",
            "Leave": "@{_matching_timestamp}{custom}",
            "Exc Time": "@{time.exc}{custom}",
            "Inc Time": "@{time.inc}{custom}",
            "Index": "@{index}",
        }
    )
    hover.formatters = {
        "@{Timestamp (ns)}": get_time_hover_formatter(),
        "@{_matching_timestamp}": get_time_hover_formatter(),
        "@{time.inc}": get_time_hover_formatter(),
        "@{time.exc}": get_time_hover_formatter(),
    }
    hover.renderers = [hbar, scatter] if instant_events else [hbar]
    hover.callback = CustomJS(
        code="""
        let hbar_tooltip = document.querySelector('.bk-tooltip');
        let scatter_tooltip = hbar_tooltip.nextElementSibling;

        if (hbar_tooltip && scatter_tooltip &&
            hbar_tooltip.style.display != 'none' &&
            scatter_tooltip.style.display != 'none')
        {
            hbar_tooltip.style.display = 'none';
        }
    """
    )

    # Return plot with wrapper function
    return plot(p)


def comm_matrix(trace, kind="heatmap", mapping="linear", labels=False, **kwargs):
    """Displays the result of Trace.comm_matrix as either a heatmap or scatterplot

    If kind == "heatmap", displays an image that encodes communication volume as color
    intensity. If kind == "scatterplot", displays circles whose areas represent the
    communcation voulme.
    """
    comm_matrix = trace.comm_matrix(**kwargs)
    is_size = kwargs.pop("output", "size") == "size"

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
            palette=RdYlBu11, low=0, high=np.amax(comm_matrix)
        )
    elif mapping == "log":
        color_mapper = LogColorMapper(
            palette=RdYlBu11,
            low=max(np.amin(comm_matrix), 1),
            high=np.amax(comm_matrix),
        )
    else:
        color_mapper = LinearColorMapper(palette="RdYlBu11", low=1, high=1)

    # Create Bokeh plot
    p = figure(
        # title="Communication Matrix",
        x_axis_label="Sender",
        y_axis_label="Receiver",
        x_range=(-0.5, N - 0.5),
        y_range=(N - 0.5, -0.5),
        x_axis_location="above",
        tools="hover,pan,reset,wheel_zoom,save",
        # sizing_mode="stretch_width",
        width=get_height(N, 300) + 150,
        height=get_height(N, 300),
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
            formatter=get_size_tick_formatter() if is_size else BasicTickFormatter(),
            border_line_color=None,
        )
        p.add_layout(color_bar, "right")

        if labels:
            stacked["color"] = np.where(
                abs(stacked["volume"] / stacked["volume"].max() - 0.5) < 0.1,
                "black",
                "white",
            )
            stacked["volume_formatted"] = stacked["volume"].apply(
                lambda x: "%.2f" % (x / 1e6)
            )
            labels = LabelSet(
                x="sender",
                y="receiver",
                text="volume_formatted",
                source=ColumnDataSource(stacked[stacked["volume"] > 0]),
                text_align="center",
                text_font_size="9px",
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
        base=2, desired_num_ticks=min(N, 16), min_interval=1, num_minor_ticks=0
    )
    p.yaxis.ticker = BasicTicker(
        base=2, desired_num_ticks=min(N, 16), min_interval=1, num_minor_ticks=0
    )
    # p.xaxis.major_label_orientation = math.pi / 6 if N > 12 else "horizontal"
    # p.xaxis.formatter = get_process_tick_formatter()
    # p.yaxis.formatter = get_process_tick_formatter()

    # Configure hover
    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Sender": "Process $x{0.}",
            "Receiver": "Process $y{0.}",
            "Bytes": "@image{custom}",
        }
        if is_size
        else {
            "Sender": "Process $x{0.}",
            "Receiver": "Process $y{0.}",
            "Count": "@image",
        }
    )
    hover.formatters = {"@image": get_size_hover_formatter()}

    # Return plot with wrapper function
    return plot(p)


def message_histogram(trace, **kwargs):
    """Displays the result of Trace.message_histogram as a bar graph

    The heights of the bars represent the frequency of messages per size range."""
    hist, edges = trace.message_histogram(**kwargs)

    # Create bokeh plot
    p = figure(
        # title="Message Histogram",
        y_range=(0, np.max(hist) + np.max(hist) / 4),
        x_axis_label="Message size",
        y_axis_label="Number of messages",
        tools="hover,save",
        sizing_mode="stretch_width",
    )

    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:], line_color="white")

    p.xgrid.visible = False
    p.xaxis.formatter = get_size_tick_formatter()
    p.xaxis.minor_tick_line_color = None
    p.yaxis.formatter = NumeralTickFormatter()
    # p.xaxis.ticker = AdaptiveTicker()

    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {"Bin": "@left{custom} - @right{custom}", "Number of messages": "@top"}
    )
    hover.formatters = {
        "@left": get_size_hover_formatter(),
        "@right": get_size_hover_formatter(),
    }

    return plot(p)


def time_profile(trace, **kwargs):
    """Displays the result of Trace.time_profile as a stacked bar graph

    The bars are color-coded by the function name, and their heights represent
    the exclusive time spent by each function in each bin."""
    normalized = kwargs.get("normalized")
    profile = trace.time_profile(**kwargs).fillna(0)

    # Move "Idle" to the top
    if "Idle" in profile.columns:
        profile.insert(len(profile.columns) - 1, "Idle", profile.pop("Idle"))

    # Get function names
    func = profile.columns[3:]
    func_trimmed = trimmed(func).tolist()

    # Create Bokeh plot
    p = figure(
        # title="Time Profile",
        x_axis_label="Time",
        y_axis_label="Time Contribution",
        tools="hover,save",
        sizing_mode="stretch_width",
        height=max(400, 20 * len(profile.columns)),
    )

    # Prepare colors
    palette = get_palette(trace)

    # Add stacked bars
    bin_size = profile.loc[0]["bin_start"] - profile.loc[0]["bin_end"]
    p.vbar_stack(
        func,
        x=dodge("bin_start", -bin_size / 2, p.x_range),
        width=bin_size,
        color=[palette[f] for f in func],
        source=profile,
        legend_label=func_trimmed,
        fill_alpha=1.0,
        line_width=1,
    )

    # Additional plot config
    p.yaxis.formatter = (
        get_percent_tick_formatter() if normalized else get_time_tick_formatter()
    )
    p.xaxis.formatter = get_time_tick_formatter()
    p.y_range.start = 0
    p.xgrid.grid_line_color = None
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None

    # Move legend to right side
    p.legend[0].items = list(reversed(p.legend[0].items))
    p.add_layout(p.legend[0], "right")

    # Configure hover
    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Name": "$name",
            "Bin": "@bin_start{custom} - @bin_end{custom}",
            "Time Spent": "@$name{custom}",
        }
    )
    hover.formatters = {
        **{
            "@{" + f + "}": get_percent_hover_formatter()
            if normalized
            else get_time_hover_formatter()
            for f in func
        },
        "@bin_start": get_time_hover_formatter(),
        "@bin_end": get_time_hover_formatter(),
    }

    # Return plot with wrapper function
    return plot(p)


def flat_profile(trace, x_axis_type="linear", **kwargs):
    trace.calc_exc_metrics(["Timestamp (ns)"])

    profile = (
        trace.flat_profile(metrics=["time.exc"], **kwargs)
        .reset_index()
        .sort_values(["time.exc"], ascending=True)
    )
    y_range = profile["Name"].tolist()

    p = figure(
        # title="Flat Profile",
        y_range=y_range,
        x_range=[min(profile["time.exc"]) / 2, max(profile["time.exc"] * 2)]
        if x_axis_type == "log"
        else [0, max(profile["time.exc"])],
        x_axis_label="Total Exc Time",
        y_axis_label="Function Name",
        tools="hover,save",
        sizing_mode="stretch_width",
        x_axis_type=x_axis_type,
        height=get_height(len(y_range), 400),
    )

    p.hbar(
        y="Name",
        left=min(profile["time.exc"]) / 2,
        right="time.exc",
        source=profile,
        height=0.7,
        color=get_factor_cmap("Name", trace),
    )

    p.ygrid.grid_line_color = None
    p.xaxis.formatter = get_time_tick_formatter()
    p.yaxis.formatter = get_trimmed_tick_formatter()

    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {"Name": "@Name", "Total Exc Time": "@{time.exc}{custom}"}
    )
    hover.formatters = {"@{time.exc}": get_time_hover_formatter()}

    return plot(p)


def comm_over_time(trace, **kwargs):
    hist, edges = trace.comm_over_time(**kwargs)
    is_size = kwargs.pop("output", "size") == "size"

    p = figure(
        y_range=(0, np.max(hist) + np.max(hist) / 4),
        x_axis_label="Time",
        y_axis_label="Total volume sent",
        tools="hover,save",
        sizing_mode="stretch_width",
    )
    p.xaxis.formatter = get_time_tick_formatter()
    p.yaxis.formatter = get_size_tick_formatter()

    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:], line_color="white")

    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Bin": "@left{custom} - @right{custom}",
            "Total volume sent:": "@top{custom}",
        }
        if is_size
        else {"Bin": "@left{custom} - @right{custom}", "Number of messages": "@top"}
    )
    hover.formatters = {
        "@left": get_time_hover_formatter(),
        "@right": get_time_hover_formatter(),
        "@top": get_size_hover_formatter(),
    }

    return plot(p)


def comm_summary(trace, **kwargs):
    summary = trace.comm_summary(**kwargs).reset_index()
    is_size = kwargs.pop("output", "size") == "size"

    p = figure(
        # y_range=(np.min(sends) * 0.8, np.max(sends) * 1.05),
        x_range=(-0.5, len(summary) - 0.5),
        x_axis_label="Process",
        y_axis_label="Volume",
        sizing_mode="stretch_width",
        tools="hover,save",
    )
    p.y_range.start = 0

    p.xgrid.visible = False
    p.yaxis.formatter = get_size_tick_formatter()
    p.xaxis.ticker = BasicTicker(
        base=2,
        desired_num_ticks=min(len(trace.events["Process"].unique()), 16),
        min_interval=1,
        num_minor_ticks=0,
    )

    p.vbar(
        x=dodge("Process", -0.1667, range=p.x_range),
        top="Sent",
        width=0.2,
        source=summary,
        color=get_palette(trace)["MPI_Send"],
        legend_label="Total sent",
    )
    p.vbar(
        x=dodge("Process", 0.1667, range=p.x_range),
        top="Received",
        width=0.2,
        source=summary,
        color=get_palette(trace)["MPI_Recv"],
        legend_label="Total received",
    )
    p.add_layout(p.legend[0], "right")

    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Process": "@Process",
            "Sent": "@Sent{custom}",
            "Received": "@Received{custom}",
        }
        if is_size
        else {"Process": "@Process", "Sent": "@Sent", "Received": "@Received"}
    )
    hover.formatters = {
        "@Sent": get_size_hover_formatter(),
        "@Received": get_size_hover_formatter(),
    }

    plot(p)
