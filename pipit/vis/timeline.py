import pandas as pd
import holoviews as hv
from holoviews import opts, streams
from bokeh.models import HoverTool, PrintfTickFormatter
from pipit.util import clamp, formatter, vis_init
import random

# Min fraction of viewport an event has to occupy to be drawn
min_viewport_percentage = 1 / 1920

default_palette_list = [
    "rgb(138,113,152)",
    "rgb(175,112,133)",
    "rgb(127,135,225)",
    "rgb(93,81,137)",
    "rgb(116,143,119)",
    "rgb(178,214,122)",
    "rgb(87,109,147)",
    "rgb(119,155,95)",
    "rgb(114,180,160)",
    "rgb(132,85,103)",
    "rgb(157,210,150)",
    "rgb(148,94,86)",
    "rgb(164,108,138)",
    "rgb(139,191,150)",
    "rgb(110,99,145)",
    "rgb(80,129,109)",
    "rgb(125,140,149)",
    "rgb(93,124,132)",
    "rgb(140,85,140)",
    "rgb(104,163,162)",
    "rgb(132,141,178)",
    "rgb(131,105,147)",
    "rgb(135,183,98)",
    "rgb(152,134,177)",
    "rgb(141,188,141)",
    "rgb(133,160,210)",
    "rgb(126,186,148)",
    "rgb(112,198,205)",
    "rgb(180,122,195)",
    "rgb(203,144,152)",
]

random.shuffle(default_palette_list)
default_palette = tuple(default_palette_list)


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
    df["y0"] = df["Rank"] - (dividend / 2)
    df["y1"] = df["Rank"] + (dividend / 2)
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

        # Filter by sends and receives
        sends = trace.events[(trace.events["Event"] == "MpiSend")].head(5)
        recvs = trace.events[trace.events["Event"] == "MpiRecv"].head(5)

        print(sends)

        both = pd.concat([sends, recvs])

        # Nodes
        x = both["Timestamp (ns)"] * 1e-9
        y = both["Location ID"].astype(int)
        node_indices = both.index.values

        # Edges
        source = sends.index.values
        target = recvs.index.values
        size = sends["Attributes"].apply(lambda x: x["msg_length"])

        min_weight = 0.5
        max_weight = 6

        norm = (
            (size - size.min()) / (size.max() - size.min()) * max_weight
        ) + min_weight

        rects = hv.Rectangles(filtered, ["Enter", "y0", "Leave", "y1"])
        graph = hv.Graph(
            ((source, target, size, norm), (x, y, node_indices)), vdims=["size", "norm"]
        )

        # filtered_labels = filtered[
        #     (filtered["Leave"] > x_min) &
        #     (filtered["Enter"] < x_max) &
        #     (filtered["Inc Time (s)"] > (min_inc_time*40))
        # ].copy()

        # filtered_labels["label_x"] = filtered_labels["mid"].clip(x_min, x_max)
        # filtered_labels["stop"] = round((filtered_labels["Inc Time (s)"] / viewport_size) * 80)
        # filtered_labels["stop"] =  filtered_labels["stop"].astype("int")
        # filtered_labels["label"] = filtered_labels.apply(lambda x: x["Name"][:x["stop"]], 1)

        # labels = hv.Labels(
        #     filtered_labels, ["label_x", "Rank"], "label"
        # ).opts(text_font_size="9pt")

        return rects #* graph #* labels

    # Generate DynamicMap
    rangeX = streams.RangeX()
    dmap = hv.DynamicMap(get_rects, streams=[rangeX])
    return dmap.opts(
        opts.Graph(node_size=0, edge_line_width="norm", tools=[], edge_color="k"),
        opts.Rectangles(
            active_tools=["xwheel_zoom"],
            cmap=cmap,
            default_tools=["xpan", "xwheel_zoom"],
            height=clamp(len(df["Rank"].unique()) * 20 + 100, 150, 1000),
            invert_yaxis=True,
            line_width=0.2,
            line_color="white",
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
