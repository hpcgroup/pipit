import numpy as np
import pandas as pd
from bokeh.models import (
    Arrow,
    ColumnDataSource,
    CustomJS,
    CustomJSTickFormatter,
    FixedTicker,
    Grid,
    HoverTool,
    OpenHead,
    WheelZoomTool,
)
from bokeh.events import RangesUpdate, Tap
from bokeh.plotting import figure
from bokeh.transform import dodge

import pipit as pp
from pipit.vis.util import (
    factorize_tuples,
    get_factor_cmap,
    get_html_tooltips,
    get_time_hover_formatter,
    get_time_tick_formatter,
    show,
    trimmed,
)


def prepare_data(trace: pp.Trace, show_depth: bool, instant_events: bool):
    """Prepare data for plotting the timeline."""
    # Generate necessary metrics
    trace.calc_exc_metrics(["Timestamp (ns)"])
    trace._match_events()
    trace._match_caller_callee()

    # Prepare data for plotting
    events = (
        trace.events[
            trace.events["Event Type"].isin(
                ["Enter", "Instant"] if instant_events else ["Enter"]
            )
        ]
        .sort_values(by="time.inc", ascending=False)
        .copy(deep=False)
    )

    # Determine y-coordinates from process and depth
    y_tuples = (
        list(zip(events["Process"], events["_depth"]))
        if show_depth
        else list(zip(events["Process"]))
    )

    codes, y_tuples = factorize_tuples(y_tuples)
    events["y"] = codes
    num_ys = len(y_tuples)

    events["_depth"] = events["_depth"].astype(float).fillna("")
    events["name_trimmed"] = trimmed(events["Name"])
    events["_matching_event"] = events["_matching_event"].fillna(-1)

    # Only select a subset of columns for plotting
    events = events[
        [
            "Timestamp (ns)",
            "_matching_timestamp",
            "_matching_event",
            "y",
            "Name",
            "time.inc",
            "Process",
            "time.exc",
            "name_trimmed",
            "Event Type",
        ]
    ]

    return events, y_tuples, num_ys


def update_cds(
    event: RangesUpdate,
    events: pd.DataFrame,
    instant_events: bool,
    hbar_source: ColumnDataSource,
    scatter_source: ColumnDataSource,
) -> None:
    """
    Callback function that updates the 3 data sources (hbar_source, scatter_source,
    image_source) based on the new range.

    Called when user zooms or pans the timeline (and once initially).
    """

    x0 = event.x0 if event is not None else events["Timestamp (ns)"].min()
    x1 = event.x1 if event is not None else events["Timestamp (ns)"].max()

    x0 = x0 - (x1 - x0) * 0.25
    x1 = x1 + (x1 - x0) * 0.25

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
    large = func
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


def tap_callback(
    event: Tap,
    events: pd.DataFrame,
    trace: pp.Trace,
    show_depth: bool,
    p: figure,
) -> None:
    """
    Callback function that adds an MPI message arrow when user clicks
    on a send or receive event.
    """
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


def plot_timeline(
    trace: pp.Trace,
    show_depth: bool = False,
    instant_events: bool = False,
    critical_path: bool = False,
):
    """
    Displays the events of a trace on a timeline.

    Instant events are drawn as points, function calls are drawn as horizontal bars,
    and MPI messages are drawn as arrows.

    Args:
        trace: The trace to be visualized.
        show_depth: Whether to show the depth of the function calls.
        instant_events: Whether to show instant events.
        critical_path: Whether to show the critical path. NOTE: critical_path currently
            only works when show_depth==False. TODO: make it work with show_depth=True.

    Returns:
        The Bokeh plot.
    """

    # Prepare data to be plotted
    events, y_tuples, num_ys = prepare_data(trace, show_depth, instant_events)

    # Define the 3 data sources (Bokeh ColumnDataSource)
    hbar_source = ColumnDataSource(events.head(0))
    scatter_source = ColumnDataSource(events.head(0))
    image_source = ColumnDataSource(
        data=dict(
            image=[np.zeros((50, 16), dtype=np.uint32)], x=[0], y=[0], dw=[0], dh=[0]
        )
    )

    # Create Bokeh plot
    plot_height = 120 + 22 * num_ys
    min_ts, max_ts = events["Timestamp (ns)"].min(), events["Timestamp (ns)"].max()

    p = figure(
        x_range=(min_ts, max_ts + (max_ts - min_ts) * 0.05),
        y_range=(num_ys - 0.5, -0.5),
        x_axis_location="above",
        tools="hover,xpan,reset,xbox_zoom,xwheel_zoom,save",
        output_backend="webgl",
        height=min(500, plot_height),
        sizing_mode="stretch_width",
        toolbar_location=None,
        x_axis_label="Time",
    )

    # Define color mappings
    fill_cmap = get_factor_cmap("Name", trace)
    line_cmap = get_factor_cmap("Name", trace, scale=0.7)

    # Add glyphs
    # Bars for "large" functions
    hbar = p.hbar(
        left="Timestamp (ns)",
        right="_matching_timestamp",
        y="y",
        height=0.8 if show_depth else 0.8,
        source=hbar_source,
        fill_color=fill_cmap,
        line_color=line_cmap,
        line_width=1,
        line_alpha=0.5,
        legend_field="name_trimmed",
    )

    # Image for small functions
    p.image_rgba(source=image_source)

    # Scatter for instant events
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

    # Lines for critical path
    if critical_path:
        critical_dfs = trace.critical_path_analysis()
        for df in critical_dfs:
            p.line(
                x="Timestamp (ns)",
                y="Process",
                source=ColumnDataSource(df),
                line_width=2,
                line_color="black",
                legend_label="Critical Path",
                level="overlay",
            )

    # Additional plot config
    p.toolbar.active_scroll = p.select(dict(type=WheelZoomTool))[0]

    # Grid config
    depth_ticks = np.arange(0, num_ys)
    process_ticks = np.array(
        [i for i, v in enumerate(y_tuples) if len(v) == 1 or v[1] == 0]
    )
    p.ygrid.visible = False
    g1 = Grid(
        dimension=1,
        grid_line_color="white",
        grid_line_width=2 if show_depth else 2,
        ticker=FixedTicker(
            ticks=np.concatenate([depth_ticks - 0.49, depth_ticks + 0.49])
        ),
        level="glyph",
    )
    g2 = Grid(
        dimension=1,
        grid_line_width=2,
        band_fill_color="gray",
        band_fill_alpha=0.1,
        ticker=FixedTicker(ticks=process_ticks - 0.5),
        level="glyph",
    )
    p.add_layout(g1)
    p.add_layout(g2)

    # Axis config
    p.xaxis.formatter = get_time_tick_formatter()
    p.yaxis.formatter = CustomJSTickFormatter(
        args={
            "y_tuples": y_tuples,
        },
        code="""
            return "Process " + y_tuples[Math.floor(tick)][0];
        """,
    )
    p.yaxis.ticker = FixedTicker(ticks=process_ticks + 0.1)
    p.yaxis.major_tick_line_color = None

    # Legend config
    p.add_layout(p.legend[0], "below")
    p.legend.orientation = "horizontal"
    p.legend.location = "center"
    p.legend.nrows = 2

    # Hover config
    hover = p.select(HoverTool)
    hover.tooltips = get_html_tooltips(
        {
            "Name": "@Name",
            # "Process": "@Process",
            "Enter": "@{Timestamp (ns)}{custom} [@{index}]",
            "Leave": "@{_matching_timestamp}{custom} [@{_matching_event}]",
            "Time (Inc)": "@{time.inc}{custom}",
            "Time (Exc)": "@{time.exc}{custom}",
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

    # Add interactive callbacks (these happen on the Python side)
    p.on_event(
        RangesUpdate,
        lambda event: update_cds(
            event, events, instant_events, hbar_source, scatter_source
        ),
    )
    p.on_event(Tap, lambda event: tap_callback(event, events, trace, show_depth, p))

    # Make initial call to callback
    update_cds(None, events, instant_events, hbar_source, scatter_source)

    # Return plot
    return show(p)