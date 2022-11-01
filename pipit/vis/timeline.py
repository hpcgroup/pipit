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

FUNCTIONAL_EVENTS_TOOLTIPS = """
    <div>
        <span style="font-weight: bold;">Name:</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@Name</span>
    </div>
    <div>
        <span style="font-weight: bold;">Inc Time (Total):</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@{humanized_inc_time}</span>
    </div>
    <div>
        <span style="font-weight: bold;">Exc Time (Self):</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@{humanized_exc_time}</span>
    </div>
"""

COMMUNICATION_EVENTS_TOOLTIPS = """
"""

INSTANT_EVENTS_TOOLTIPS = """
    <div>
        <span style="font-weight: bold;">Type:</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@{Event}</span>
    </div>
    <div>
        <span style="font-weight: bold;">Timestamp:</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@{humanized_timestamp}</span>
    </div>
"""

STANDARD_COLS = [
    "Event",
    "Timestamp (ns)",
    "Name",
    "Location ID",
    "Inc Time (ns)",
    "Exc Time (ns)",
    "Matching Time",
]


def calculate_height(num_ranks):
    return clamp(num_ranks * 30 + 100, 150, 1000)


def apply_bokeh_customizations(plot, _):
    plot.state.toolbar_location = "above"
    plot.state.ygrid.visible = False
    plot.state.legend.label_text_font_size = "8pt"
    plot.state.legend.spacing = 0
    plot.state.legend.location = "top"


def timeline(trace, palette=DEFAULT_PALETTE, ranks=None, max_ranks=16):
    """Generates interactive timeline of events in a Trace instance.

    Args:
        trace: Trace instance whose events are being visualized
        palette: Color palette used to encode the functions
        ranks: List or range of ranks to include in timeline
        max_ranks: Maximum number of ranks to include in timeline

    Returns:
        hv.DynamicMap: Instance of HoloViews DynamicMap object, which can be
            displayed in a notebook
    """
    # Initialize vis
    vis_init()

    # Calculate inc and exc times if not already done
    if "Inc Time (ns)" not in trace.events:
        trace.calculate_inc_time()

    if "Exc Time (ns)" not in trace.events:
        trace.calculate_exc_time()

    events = trace.events[STANDARD_COLS].copy(deep=False)
    events["Timestamp (ms)"] = events["Timestamp (ns)"] / 1e6
    events["Matching Time"] = events["Matching Time"] / 1e6

    # Filter by ranks
    n_ranks = events["Location ID"].astype("int").max() + 1
    dividend = max(1, round(n_ranks / max_ranks))
    events = events[(events["Location ID"].astype("int")) % dividend == 0]

    # Initial viewport range
    default_x_min = events["Timestamp (ms)"].min()
    default_x_max = events["Timestamp (ms)"].max()

    # Do some preprocessing for Holoviews elements
    # Functional events
    func = events[events["Event"] == "Enter"].copy(deep=False)
    func["y"] = func["Location ID"].astype("int")
    func["y0"] = func["y"] - (dividend / 2)
    func["y1"] = func["y"] + (dividend / 2)
    func["humanized_exc_time"] = func["Exc Time (ns)"].apply(humanize_timedelta)
    func["humanized_inc_time"] = func["Inc Time (ns)"].apply(humanize_timedelta)

    func_cmap = generate_cmap(func["Name"], palette, True)

    # Communication events
    send = events[(events["Event"] == "MpiSend") | (events["Event"] == "MpiIsend")]
    recv = events[(events["Event"] == "MpiRecv") | (events["Event"] == "MpiIrecv")]

    comm = pd.DataFrame()
    comm["x0"] = send["Timestamp (ns)"].values
    comm["y0"] = send["Location ID"].values
    comm["x1"] = recv["Timestamp (ns)"].values
    comm["y1"] = recv["Location ID"].values

    # Instant events
    inst = events[(events["Event"] != "Enter") & (events["Event"] != "Leave")].copy(
        deep=False
    )
    inst["humanized_timestamp"] = inst["Timestamp (ns)"].apply(humanize_timedelta)
    # inst_cmap = generate_cmap(inst["Event"], palette, True)

    # DynamicMap callback
    def get_elements(x_range):
        if x_range is None or pd.isna(x_range[0]) or pd.isna(x_range[1]):
            x_range = (default_x_min, default_x_max)

        x_min, x_max = x_range
        viewport_size = x_max - x_min

        x_min_buff = x_min - (viewport_size * 0.25)
        x_max_buff = x_max + (viewport_size * 0.25)
        min_width = viewport_size * MIN_VIEWPORT_PERCENTAGE

        # Filter dataframes based on x_range
        inst_filtered = inst[
            (inst["Timestamp (ms)"] > x_min_buff)
            & (inst["Timestamp (ms)"] < x_max_buff)
        ]

        if len(inst_filtered) > 5000:
            inst_filtered = inst_filtered.sample(n=5000)

        func_filtered = func[
            (func["Matching Time"] > x_min_buff)
            & (func["Timestamp (ms)"] < x_max_buff)
            & (func["Exc Time (ns)"] * 1e-6 > min_width)
        ]

        if len(func_filtered) > 5000:
            func_filtered = func_filtered.sample(n=5000)

        # comm_filtered = comm[
        #     ((comm["x0"] < x_max_buff) & (comm["x1"] > x_min_buff))
        #     & (comm["x1"] - comm["x0"] > min_width)
        # ]

        points = hv.Points(inst_filtered, ["Timestamp (ms)", "Location ID"])
        rects = hv.Rectangles(
            func_filtered, ["Timestamp (ms)", "y0", "Matching Time", "y1"]
        )
        # segments = hv.Segments(comm_filtered, ["x0", "y0", "x1", "y1"])

        return rects * points

    # Generate DynamicMap based on above callback
    rangeX = streams.RangeX()
    dmap = hv.DynamicMap(get_elements, streams=[rangeX])

    return dmap.opts(
        opts.Points(
            # color="Event",
            # cmap=inst_cmap,
            size=6,
            # line_color="rgba(0,0,0,0.8)",
            color="rgba(60, 60, 60, 0.3)",
            # alpha=0.3,
            tools=[
                HoverTool(tooltips=INSTANT_EVENTS_TOOLTIPS, point_policy="follow_mouse")
            ],
        ),
        opts.Segments(color="black"),
        opts.Rectangles(
            active_tools=["xwheel_zoom"],
            cmap=func_cmap,
            default_tools=["xpan", "xwheel_zoom"],
            height=calculate_height(len(events["Location ID"].unique())),
            invert_yaxis=True,
            line_width=0.2,
            line_color="white",
            responsive=True,
            # title="Events Timeline",
            xformatter=DatetimeTickFormatter(),
            xaxis="top",
            legend_position="right",
            xlabel="",
            yformatter=PrintfTickFormatter(format="Process %d"),
            ylabel="",
            yticks=events["Location ID"].astype("int").unique(),
            hooks=[apply_bokeh_customizations],
            show_grid=True,
            tools=[
                HoverTool(
                    tooltips=FUNCTIONAL_EVENTS_TOOLTIPS, point_policy="follow_mouse"
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
