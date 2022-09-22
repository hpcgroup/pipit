import holoviews as hv
from bokeh.models import HoverTool, PrintfTickFormatter
from bokeh.palettes import Category20_20 as palette
from holoviews import opts, streams
from pipit.util import formatter, vis_init

# Min fraction of viewport an event has to occupy to be drawn
min_viewport_percentage = 1 / 3840


def timeline(trace):
    """Generates interactive timeline of events in a Trace instance"""

    # Initialize vis
    vis_init()

    # Calculate some column values we need for visualization
    df = trace.events.copy()
    df["Rank"] = df["Rank"].astype("int")
    df["y0"] = df["Rank"] - 0.5
    df["y1"] = df["Rank"] + 0.5
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
            <b>@Name</b> <em>($index)</em><br/>
            <b>Total:</b> @{inc_time_form} <em>(@inc_time_pct{0.00%})</em><br/>
            <b>Self:</b> @{exc_time_form} <em>(@exc_time_pct{0.00%})</em><br/>
        """,
        point_policy="follow_mouse",
    )

    # Bokeh-specific customizations
    def bokeh_hook(plot, _):
        plot.state.toolbar_location = "above"
        plot.state.ygrid.visible = False
        plot.state.legend.label_text_font_size = "9pt"

    # Callback for hv.DynamicMap
    # Generates hv.Rectangles based on current x-range
    def get_rects(x_range):
        if x_range is None:
            x_range = (df["Enter"].min(), df["Leave"].max())

        x_min, x_max = x_range
        viewport_size = x_max - x_min
        min_inc_time = min_viewport_percentage * viewport_size

        filtered = df[
            (df["Leave"] > x_min)
            & (df["Enter"] < x_max)
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
            height=len(df["Rank"].unique()) * 25 + 150,
            invert_yaxis=True,
            line_width=0.2,
            line_color="black",
            line_alpha=0.5,
            responsive=True,
            title="Events Timeline",
            xaxis="top",
            xlabel="",
            yformatter=PrintfTickFormatter(format="Process %d"),
            ylabel="",
            yticks=df["Rank"].unique(),
            hooks=[bokeh_hook],
            show_grid=True,
            tools=[hover],
            fill_color="Name",
            fontsize={
                "title": 10,
                "legend": 8,
            },
            padding=0,
        ),
    )
