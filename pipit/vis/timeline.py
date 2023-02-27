import datashader as ds
import holoviews as hv
import pandas as pd
from holoviews import opts
from holoviews.operation.datashader import datashade, spread, rasterize, dynspread
from holoviews.operation import decimate
from ._util import getProcessTickFormatter, getTimeTickFormatter
import pipit as pp
import numpy as np
from bokeh.models import Grid, FixedTicker, HoverTool
import bokeh.palettes as bp

hv.extension("bokeh")

added = False

def hook(plot, _):
    plot.handles["yaxis"].minor_tick_line_color = None
    plot.handles["yaxis"].major_tick_line_color = None

    # global added

    # if not added:
    #     plot.state.add_layout( Grid(dimension=1, ticker=FixedTicker(ticks=[-1.5, -0.5, 0.5, 1.5, ]), band_fill_alpha=0.1, band_fill_color="skyblue"))
    #     added = True


def plot_timeline(trace):
    added = False
    trace._pair_enter_leave()
    trace._gen_calling_relationships()
    trace.calc_inc_time()

    # Default opts
    ropts = dict(
        responsive=True,
        height=min(700, max(150, len(trace.events["Process"].unique()) * 40)),
        default_tools=["xpan", "xwheel_zoom"],
        active_tools=["xpan", "xwheel_zoom"],
        xaxis="top",
        labelled=[],
        xformatter=getTimeTickFormatter(),
        yformatter=getProcessTickFormatter(),
        yticks=len(trace.events["Process"].unique()),
        show_grid=True,
        gridstyle=dict(ygrid_line_color=None),
        invert_yaxis=True,
        hooks=[hook],
        title="Timeline",
        ylim=(-0.5, len(trace.events["Process"].unique()) - 0.5),
    )

    # Functions -> hv.Rectangles
    func = trace.events[trace.events["Event Type"] == "Enter"].copy(deep=False)
    func["y0"] = func["Process"].astype("float") + 0.1
    func["y1"] = func["Process"].astype("float") + 0.5
    func["Name"] = func["Name"].astype("category")

    colors = pp.config["vis"]["colors"]
    color_key = {
        cat: tuple(
            int(x) for x in colors[i].replace("rgb(", "").replace(")", "").split(",")
        )
        for i, cat in enumerate(func["Name"].unique())
    }

    rects_opts = opts.Rectangles(
        fill_color=None,
        line_width=0,
        show_legend=False,
    )

    rects = (
        hv.Rectangles(
            func, kdims=["Timestamp (ns)", "y0", "Matching Timestamp", "y1"], vdims=["Name"]
        )
        .opts(rects_opts)
        .opts(**ropts)
    )

    # Instant events -> hv.Points
    inst = trace.events[trace.events["Event Type"] == "Instant"].copy(deep=False)
    inst["Type"] = np.where(inst["Name"].str.startswith("Mpi"), "MPI", "other")
    inst["y"] = inst["Process"].astype("float") - 0.15 # + np.random.uniform(low=0, high=0.3, size=(inst.shape[0],))

    greens = bp.Greens[256][::-1][85:]

    points_opts = opts.Points(
        line_width=0,
        marker='circle',
        fill_color=None,
        size=5,
    )
    
    points = hv.Points(inst, kdims=["Timestamp (ns)", "y"], vdims=["Name"]).opts(points_opts).opts(**ropts)

    d1 = decimate(rects, max_samples=1000).opts(**ropts).opts(tools=[HoverTool(tooltips={"Name": "@Name"})])
    d2 = decimate(points, max_samples=1000).opts(**ropts).opts(tools=[HoverTool(tooltips={"Name": "@Name"})])

    r1 = datashade(rects, color_key=color_key, min_alpha=0.1, aggregator=ds.by("Name", ds.any())).opts(**ropts)
    r2 = spread(rasterize(points), px=2, shape='circle').opts(cmap=greens, clim=(0, 20), **ropts)

    return d1 * d2 * r1 * r2

    def get_elements(x_range):
        low, high = (0, rects["Matching Timestamp"].max()) if x_range is None else x_range

        # Get functions
        min_time = (high - low) / 500

        in_range = rects[~((rects["Matching Timestamp"] < low) | (rects["Timestamp (ns)"] > high))]
        large = in_range[in_range["Matching Timestamp"] - in_range["Timestamp (ns)"] >= min_time]
        small = in_range[in_range["Matching Timestamp"] - in_range["Timestamp (ns)"] < min_time]

        small_raster = datashade(
            small,
            color_key=color_key,
            min_alpha=0.1,
            aggregator=ds.by("Name", ds.any()),
            dynamic=False,
            width=800,
            height=700,
        ).opts(**ropts)

        in_range_pts = points[(points["Timestamp (ns)"] > low) & (points["Timestamp (ns)"] < high)]


        return large.opts(tools=["hover"]) * small_raster * in_range_pts

    return hv.DynamicMap(get_elements, streams=[hv.streams.RangeX()]).opts(**ropts)
