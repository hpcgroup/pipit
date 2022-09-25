import pandas as pd
import holoviews as hv
from holoviews import opts, streams
from bokeh.models import HoverTool, PrintfTickFormatter
from pipit.util import clamp, formatter, vis_init

# Min fraction of viewport an event has to occupy to be drawn
min_viewport_percentage = 1 / 1920

default_palette = (
    "#c8a83a",
    "#7662ca",
    "#5bb645",
    "#bf51b6",
    "#9db641",
    "#da4b83",
    "#5dbd83",
    "#d2404e",
    "#3fbcc1",
    "#d4502d",
    "#6488ca",
    "#e08a3c",
    "#cb89cb",
    "#3b804c",
    "#9e4a6b",
    "#68782c",
    "#e28981",
    "#bcab6c",
    "#a55236",
    "#946d2e",
)


def timeline(trace, palette=default_palette, max_ranks=16):
    """Generates interactive timeline of events in a Trace instance"""

    # Initialize vis
    vis_init()

    # Calculate inc and exc times if not already done
    if "Inc Time (ns)" not in trace.events:
        trace.calculate_inc_time()

    if "Exc Time (ns)" not in trace.events:
        trace.calculate_exc_time()

    # Filter by "Enter" events
    events = trace.events[trace.events["Event"] == "Enter"]

    # Filter by ranks
    n_ranks = events["Location ID"].astype("int").max() + 1
    dividend = max(1, round(n_ranks / max_ranks))
    events = events[(events["Location ID"].astype("int")) % dividend == 0]

    # Create a temporary DF specifically for timeline
    df = pd.DataFrame()
    df["Name"] = events["Name"]
    df["Rank"] = events["Location ID"].astype("int")
    df["Inc Time (s)"] = events["Inc Time (ns)"] * 1e-9
    df["Exc Time (s)"] = events["Exc Time (ns)"] * 1e-9
    df["Enter"] = events["Timestamp (ns)"] * 1e-9
    df["Leave"] = events["Matching Time"] * 1e-9
    df["y0"] = df["Rank"] - (dividend / 2 - 0.5)
    df["y1"] = df["Rank"] + (dividend / 2 + 0.5)
    df["mid"] = (df["Enter"] + df["Leave"]) / 2
    df["inc_time_pct"] = df["Inc Time (s)"] / df["Leave"].max()
    df["exc_time_pct"] = df["Exc Time (s)"] / df["Leave"].max()
    df["inc_time_form"] = df["Inc Time (s)"].apply(formatter)
    df["exc_time_form"] = df["Exc Time (s)"].apply(formatter)

    # Generate colormap by function
    funcs = df["Name"].unique().tolist()
    cmap = {funcs[i]: palette[i] for i in range(len(funcs))}

    # Custom tooltip and hover behavior
    hover = HoverTool(
        tooltips="""
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
        """,
        point_policy="follow_mouse",
    )

    # Bokeh-specific customizations
    def bokeh_hook(plot, _):
        plot.state.toolbar_location = "above"
        plot.state.ygrid.visible = False
        plot.state.legend.label_text_font_size = "8pt"
        plot.state.legend.location = "right"
        plot.state.legend.spacing = 0

    # Callback for hv.DynamicMap
    # Generates hv.Rectangles based on current x-range
    def get_rects(x_range):
        if x_range is None:
            x_range = (df["Enter"].min(), df["Leave"].max())

        x_min, x_max = x_range
        viewport_size = x_max - x_min
        min_inc_time = min_viewport_percentage * viewport_size

        filtered = df[
            (df["Leave"] > x_min - (viewport_size * 0.25))
            & (df["Enter"] < x_max + (viewport_size * 0.25))
            & (df["Inc Time (s)"] > min_inc_time)
        ]
        return hv.Rectangles(filtered, ["Enter", "y0", "Leave", "y1"])

    # Generate DynamicMap
    rangeX = streams.RangeX()
    dmap = hv.DynamicMap(get_rects, streams=[rangeX])
    return dmap.opts(
        opts.Rectangles(
            active_tools=["xwheel_zoom"],
            cmap=cmap,
            default_tools=["xpan", "xwheel_zoom"],
            height=clamp(len(df["Rank"].unique()) * 20 + 100, 150, 1000),
            invert_yaxis=True,
            line_width=0.35,
            line_color="black",
            responsive=True,
            title="Events Timeline",
            xaxis="top",
            legend_position="right",
            xlabel="",
            yformatter=PrintfTickFormatter(format="Process %d"),
            ylabel="",
            yticks=df["Rank"].unique(),
            hooks=[bokeh_hook],
            show_grid=True,
            tools=[hover, "xbox_zoom", "tap"],
            fill_color="Name",
            fontsize={
                "title": 10,
                "legend": 8,
            },
            padding=0,
        ),
    )
