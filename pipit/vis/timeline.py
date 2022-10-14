import pandas as pd
import holoviews as hv
from holoviews import opts, streams
from holoviews.operation import decimate
from bokeh.models import HoverTool, PrintfTickFormatter
from pipit.vis.util import DEFAULT_PALETTE, clamp, formatter, vis_init
import random

# Min % of viewport that a functional event must occupy to be displayed
MIN_VIEWPORT_PERCENTAGE = 1 / 1920

MIN_ARROW_WIDTH = 0.5
MAX_ARROW_WIDTH = 6

TOOLTIPS = """
    <div>
        <span style="font-weight: bold;">Name:</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@Name</span>
    </div>
    <div>
        <span style="font-weight: bold;">Total:</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@{inc_time_form}
            (@inc_time_pct{0.00%})
        </span>
    </div>
    <div>
        <span style="font-weight: bold;">Self:</span>&nbsp;
        <span style="font-family: Monaco, monospace;">@{exc_time_form}
            (@exc_time_pct{0.00%})
        </span>
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


def apply_bokeh_customizations(plot, _):
    plot.state.toolbar_location = "above"
    plot.state.ygrid.visible = False
    # plot.state.legend.label_text_font_size = "8pt"
    # plot.state.legend.spacing = 0


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

    events = trace.events[STANDARD_COLS]

    # Filter by ranks
    n_ranks = events["Location ID"].astype("int").max() + 1
    dividend = max(1, round(n_ranks / max_ranks))
    events = events[(events["Location ID"].astype("int")) % dividend == 0]

    # Generate colormap for functions
    funcs = events[events["Event"] == "Enter"]["Name"].unique().tolist()
    cmap = {funcs[i]: palette[i] for i in range(len(funcs))}

    # Initial viewport range
    default_x_min = events["Timestamp (ns)"].min()
    default_x_max = events["Timestamp (ns)"].max()

    # Do some preprocessing for Holoviews elements
    ## Functional events
    func = events[events["Event"] == "Enter"].copy(deep=False)
    func["y"] = func["Location ID"].astype("int")
    func["y0"] = func["y"] - (dividend / 2)
    func["y1"] = func["y"] + (dividend / 2)

    ## Communication events
    send = events[events["Event"] == "MpiSend"]
    recv = events[events["Event"] == "MpiRecv"]

    comm = pd.DataFrame()
    comm["x0"] = send["Timestamp (ns)"].values
    comm["y0"] = send["Location ID"].values
    comm["x1"] = recv["Timestamp (ns)"].values
    comm["y1"] = recv["Location ID"].values

    ## Instant events
    inst = events[(events["Event"] != "Enter") & (events["Event"] != "Leave")]

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
            (inst["Timestamp (ns)"] > x_min_buff)
            & (inst["Timestamp (ns)"] < x_max_buff)
        ]

        func_filtered = func[
            (func["Matching Time"] > x_min_buff)
            & (func["Timestamp (ns)"] < x_max_buff)
            & (func["Exc Time (ns)"] > min_width)
        ]

        comm_filtered = comm[
            ((comm["x0"] < x_max_buff) & (comm["x1"] > x_min_buff))
            & (comm["x1"] - comm["x0"] > min_width)
        ]

        points = hv.Points(inst_filtered, ["Timestamp (ns)", "Location ID"])
        rects = hv.Rectangles(
            func_filtered, ["Timestamp (ns)", "y0", "Matching Time", "y1"]
        )
        segments = hv.Segments(comm_filtered, ["x0", "y0", "x1", "y1"])

        return rects * segments * points

    # Generate DynamicMap based on above callback
    rangeX = streams.RangeX()
    dmap = hv.DynamicMap(get_elements, streams=[rangeX])

    return dmap.opts(
        opts.Points(
            color="Event",
            cmap="category20",
            size=8,
            line_color="black",
        ),
        opts.Segments(color="black"),
        opts.Rectangles(
            active_tools=["xwheel_zoom"],
            cmap=cmap,
            default_tools=["xpan", "xwheel_zoom"],
            height=clamp(len(events["Location ID"].unique()) * 20 + 100, 150, 1000),
            invert_yaxis=True,
            line_width=0.2,
            line_color="white",
            responsive=True,
            # title="Events Timeline",
            xaxis="top",
            legend_position="right",
            xlabel="",
            yformatter=PrintfTickFormatter(format="Process %d"),
            ylabel="",
            yticks=events["Location ID"].unique(),
            hooks=[apply_bokeh_customizations],
            show_grid=True,
            tools=[
                HoverTool(tooltips=TOOLTIPS, point_policy="follow_mouse"),
                "xbox_zoom",
                "tap",
            ],
            fill_color="Name",
            fontsize={
                "title": 10,
                "legend": 8,
            },
            padding=0,
        ),
    )
