import pandas as pd
import holoviews as hv
from holoviews import opts, streams
from bokeh.models import HoverTool, PrintfTickFormatter, DatetimeTickFormatter
from pipit.vis.util import (
    DEFAULT_PALETTE,
    clamp,
    generate_cmap,
    humanize_timedelta,
    vis_init,
)

# Min % of viewport that a functional event must occupy to be displayed
MIN_VIEWPORT_PERCENTAGE = 1 / 1920

MIN_ARROW_WIDTH = 0.5
MAX_ARROW_WIDTH = 6

COLUMNS = [
    "Event Type",
    "Timestamp (ns)",
    "Name",
    "Process ID",
    "Inc Time",
    "Matching Timestamp",
]


def calculate_height(num_ranks):
    return clamp(num_ranks * 30 + 100, 150, 1000)


def apply_bokeh_customizations(plot, _):
    plot.state.toolbar_location = "above"
    plot.state.ygrid.visible = False
    plot.state.legend.label_text_font_size = "8pt"
    plot.state.legend.spacing = 0
    plot.state.legend.location = "top"


def timeline(
    trace,
    ranks=None,
    max_ranks=16,
    palette=DEFAULT_PALETTE,
    rects=True,
    points=True,
    segments=True,
):
    """Generates interactive timeline of events in a Trace instance.

    Overlays 3 types of HoloViews elements, each generated dynamically
    based on current viewport:
    1. hv.Rectangles for functional events
    2. hv.Points for instant events
    3. hv.Segments for communication events

    Args:
        trace: Trace instance whose events are being visualized
        ranks: List or range of ranks to include in timeline
        max_ranks: Maximum number of ranks to include in timeline
        palette: Color palette used to encode the functions
        rects: Whether to generate hv.Rectangles for functional events
        points: Whether to generate hv.Points for instant events
        segments: Whether to generate hv.Segments for communication events

    Returns:
        hv.HoloMap: A HoloViews object that can be viewed in a notebook
    """
    # Initialize vis
    vis_init()

    # Calculate matching rows and inc time
    trace.match_rows()
    trace.calc_inc_time()

    # Copy the trace events dataframe and modify it
    events = trace.events[COLUMNS].copy(deep=False)
    events["Timestamp (ms)"] = events["Timestamp (ns)"] / 1e6
    events["Matching Timestamp"] = events["Matching Timestamp"] / 1e6

    # Filter events by ranks based on `max_ranks`
    n_ranks = events["Process ID"].astype("int").max() + 1
    dividend = max(1, round(n_ranks / max_ranks))
    events = events[(events["Process ID"].astype("int")) % dividend == 0]

    # Initial viewport range
    default_x_min = events["Timestamp (ms)"].min()
    default_x_max = events["Timestamp (ms)"].max()

    # Construct dataframes as required for HoloViews elements
    # 1. Functional events -> hv.Rectangles
    if rects:
        func = events[events["Event Type"] == "Entry"].copy(deep=False)
        func["y"] = func["Process ID"].astype("int")
        func["y0"] = func["y"] - (dividend / 2)
        func["y1"] = func["y"] + (dividend / 2)
        func["humanized_inc_time"] = func["Inc Time"].apply(humanize_timedelta)

        func_cmap = generate_cmap(func["Name"], palette)

    # 2. Instant events -> hv.Points
    if points:
        inst = events[
            (events["Event Type"] != "Entry") & (events["Event Type"] != "Exit")
        ].copy(deep=False)
        inst["event_type"] = inst["Event Type"]
        inst["humanized_timestamp"] = inst["Timestamp (ns)"].apply(humanize_timedelta)

    # 3. Communication events -> hv.Segments
    if segments:
        send = events[
            (events["Event Type"] == "MpiSend") | (events["Event Type"] == "MpiIsend")
        ]
        recv = events[
            (events["Event Type"] == "MpiRecv") | (events["Event Type"] == "MpiIrecv")
        ]

        comm = pd.DataFrame()
        comm["x0"] = send["Timestamp (ns)"].values / 1e6
        comm["y0"] = send["Process ID"].values
        comm["x1"] = recv["Timestamp (ns)"].values / 1e6
        comm["y1"] = recv["Process ID"].values

    # DynamicMap callback
    # Generate hv.Rectangles, hv.Segments, and hv.Points dynamically based on viewport
    def get_elements(x_range):
        if x_range is None or pd.isna(x_range[0]) or pd.isna(x_range[1]):
            x_range = (default_x_min, default_x_max)

        x_min, x_max = x_range
        viewport_size = x_max - x_min

        x_min_buff = x_min - (viewport_size * 0.25)
        x_max_buff = x_max + (viewport_size * 0.25)
        min_width = viewport_size * MIN_VIEWPORT_PERCENTAGE

        # Filter dataframes constructed above based on current
        # x_range, and generate HoloViews elements
        if rects:
            func_filtered = func[
                (func["Matching Timestamp"] > x_min_buff)
                & (func["Timestamp (ms)"] < x_max_buff)
                & (func["Inc Time"] * 1e-6 > min_width)
            ]

            if len(func_filtered) > 5000:
                func_filtered = func_filtered.sample(n=5000)

            hv_rects = hv.Rectangles(
                func_filtered, ["Timestamp (ms)", "y0", "Matching Timestamp", "y1"]
            )

        if points:
            inst_filtered = inst[
                (inst["Timestamp (ms)"] > x_min_buff)
                & (inst["Timestamp (ms)"] < x_max_buff)
            ]

            if len(inst_filtered) > 5000:
                inst_filtered = inst_filtered.sample(n=5000)

            hv_points = hv.Points(inst_filtered, ["Timestamp (ms)", "Process ID"])

        if segments:
            comm_filtered = comm[
                ((comm["x0"] < x_max_buff) & (comm["x1"] > x_min_buff))
                & (comm["x1"] - comm["x0"] > min_width)
            ]

            hv_segments = hv.Segments(comm_filtered, ["x0", "y0", "x1", "y1"])

        # Generate HoloViews elements from filtered dataframes
        return (
            (hv_rects if rects else hv.Curve([]))
            * (hv_points if points else hv.Curve([]))
            * (hv_segments if segments else hv.Curve([]))
        )

    # Return DynamicMap that uses above callback
    dmap = hv.DynamicMap(get_elements, streams=[streams.RangeX()])
    return dmap.opts(
        opts.Points(
            size=6,
            color="rgba(60, 60, 60, 0.3)",
            tools=[
                HoverTool(
                    tooltips={
                        "Event Type": "@{Event_Type}",
                        "Timestamp": "@humanized_timestamp"
                    }
                )
            ],
        ),
        opts.Segments(color="black"),
        opts.Rectangles(
            active_tools=["xwheel_zoom"],
            cmap=func_cmap,
            default_tools=["xpan", "xwheel_zoom"],
            height=calculate_height(len(events["Process ID"].unique())),
            invert_yaxis=True,
            line_width=0.2,
            line_color="white",
            responsive=True,
            title="Overview timeline",
            xformatter=DatetimeTickFormatter(),
            xaxis="top",
            legend_position="right",
            xlabel="",
            yformatter=PrintfTickFormatter(format="Process %d"),
            ylabel="",
            yticks=events["Process ID"].astype("int").unique(),
            hooks=[apply_bokeh_customizations],
            show_grid=True,
            tools=[
                HoverTool(
                    point_policy="follow_mouse",
                    tooltips={
                        "Name": "@Name",
                        "Inc Time": "@humanized_inc_time"
                    }
                ),
                "xbox_zoom",
                "tap",
            ],
            fill_color="Name",
            fontsize={
                "title": 10,
                "legend": 8,
            },
            padding=(0, (0, 0.5)),
        ),
    )
