import datashader as ds
import holoviews as hv
import pandas as pd
from holoviews import opts
from holoviews.operation.datashader import datashade, spread
from holoviews.operation import decimate
from ._util import getProcessTickFormatter, getTimeTickFormatter
from bokeh.models import Grid, FixedTicker
import numpy as np
import pipit as pp

hv.extension("bokeh")

added = False


def plot_hook(plot, element):
    global added
    plot.handles["yaxis"].minor_tick_line_color = None
    plot.handles["yaxis"].major_tick_line_color = None

    if not added:
        g = Grid(
            dimension=1,
            band_fill_color="gray",
            band_fill_alpha=0.1,
            ticker=FixedTicker(ticks=list(np.arange(-1000, 1000) + 0.5)),
        )
        plot.state.add_layout(g)
        added = True


ropts = dict(
    responsive=True,
    height=800,
    default_tools=["xpan", "xwheel_zoom"],
    active_tools=["xpan", "xwheel_zoom"],
    xaxis="top",
    labelled=[],
    xformatter=getTimeTickFormatter(),
    yformatter=getProcessTickFormatter(),
    yticks=16,
    show_grid=True,
    gridstyle=dict(ygrid_line_color=None),
    hooks=[plot_hook],
    invert_yaxis=True,
    ylim=(-0.5, 15.5),
)


def plot_timeline(trace):
    trace._pair_enter_leave()
    trace._gen_calling_relationships()
    # trace.calc_inc_time()

    func = trace.events[trace.events["Event Type"] == "Enter"]

    colors = pp.config["vis"]["colors"]
    color_key = {
        cat: tuple(
            int(x) for x in colors[i].replace("rgb(", "").replace(")", "").split(",")
        )
        for i, cat in enumerate(func["Name"].unique())
    }
    color_key_ds = {
        key: tuple(int(x * 0.85) for x in value) for key, value in color_key.items()
    }

    rects_df = pd.DataFrame()
    rects_df["x0"] = func["Timestamp (ns)"]
    rects_df["y0"] = func["Process"].astype("float") - 0.4
    rects_df["x1"] = func["Matching Timestamp"]
    rects_df["y1"] = func["Process"].astype("float") + 0.4
    rects_df["name"] = func["Name"].astype("category")

    rects_opts = opts.Rectangles(
        fill_color="name",
        line_width=0.2,
        line_color="black",
        cmap=color_key,
        show_legend=False,
    )

    rects = hv.Rectangles(rects_df, vdims=["name"]).opts(rects_opts).opts(**ropts)

    def get_elements(x_range):
        low, high = (0, rects["x1"].max()) if x_range is None else x_range
        min_time = (high - low) / 500

        in_range = rects[~((rects["x1"] < low) | (rects["x0"] > high))]
        large = in_range[in_range["x1"] - in_range["x0"] >= min_time]
        small = in_range[in_range["x1"] - in_range["x0"] < min_time]

        small_raster = datashade(
            small,
            color_key=color_key_ds,
            min_alpha=0.1,
            aggregator=ds.by("name", ds.any()),
            dynamic=False,
        )

        return large.opts(tools=["hover"]) * small_raster.opts(**ropts)

    return hv.DynamicMap(get_elements, streams=[hv.streams.RangeX()]).opts(**ropts)


def plot_timeline_2(trace):
    trace._pair_enter_leave()

    global added
    added = False
    inst = trace.events[trace.events["Event Type"] == "Instant"].copy(deep=False)
    inst["y"] = inst["Process"].astype("float") - 0.2

    func = trace.events[trace.events["Event Type"] == "Enter"].copy(deep=False)
    func["ys0"] = func["Process"].astype("float") + 0.2
    func["ys1"] = func["Process"].astype("float") + 0.2
    func["yr0"] = func["Process"].astype("float") + 0
    func["yr1"] = func["Process"].astype("float") + 0.4
    func["Name"] = func["Name"].astype("category")

    # print(inst)

    sends = inst[inst["Name"].isin(["MpiSend"])]
    recvs = inst[inst["Name"].isin(["MpiRecv"])]
    comm = pd.DataFrame()
    comm["x0"] = pd.Series(sends["Timestamp (ns)"].values)
    comm["y0"] = pd.Series(sends["Process"].astype("float").values)
    comm["x1"] = pd.Series(recvs["Timestamp (ns)"].values)
    comm["y1"] = pd.Series(recvs["Process"].astype("float").values)

    segments = hv.Segments(comm).opts(
        line_width=1, line_color=None, hover_line_color="black"
    )

    points = hv.Points(inst, kdims=["Timestamp (ns)", "y"], vdims=["Name"]).opts(
        line_color="black", line_alpha=0, hover_line_alpha=1, alpha=0, size=6
    )
    rects = hv.Rectangles(
        func,
        kdims=["Timestamp (ns)", "yr0", "Matching Timestamp", "yr1"],
        vdims=["Name"],
    ).opts(alpha=0, line_alpha=0, hover_line_alpha=1, line_color="black")

    r1 = datashade(
        rects,
        min_alpha=255,
        aggregator=ds.count_cat("Name"),
    ).opts(**ropts)
    r2 = spread(
        datashade(
            points,
            min_alpha=255,
            aggregator=ds.count_cat("Name"),
        ),
        px=2,
    ).opts(**ropts)
    r3 = decimate(rects).opts(tools=["hover"]).opts(**ropts)
    r4 = decimate(points).opts(tools=["hover"]).opts(**ropts)
    # r5 = decimate(segments).opts(**ropts)

    r6 = datashade(segments, alpha=30, cmap="viridis").opts(**ropts)

    return (r6 * r1 * r2 * r3 * r4).opts(**ropts)
