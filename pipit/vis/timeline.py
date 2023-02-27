import datashader as ds
import holoviews as hv
import pandas as pd
from holoviews import opts
from holoviews.operation.datashader import datashade
from holoviews.operation import decimate
from ._util import getProcessTickFormatter, getTimeTickFormatter
import pipit as pp
import numpy as np

hv.extension("bokeh")


def hook(plot, _):
    plot.handles["yaxis"].minor_tick_line_color = None


def plot_timeline(trace):
    trace._pair_enter_leave()
    trace._gen_calling_relationships()
    trace.calc_inc_time()

    # Default opts
    ropts = dict(
        responsive=True,
        height=min(700, max(160, len(trace.events["Process"].unique()) * 40)),
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
    func["y0"] = func["Process"].astype("float") - 0.5
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
        fill_color="Name",
        line_width=0.2,
        line_color="black",
        cmap=color_key,
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
    inst["Process"] = inst["Process"].astype("float")

    points = hv.Points(inst, kdims=["Timestamp (ns)", "Process"], vdims=["Name"]).opts(**ropts)
    return rects * points

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
            width=800,
            height=700,
        ).opts(**ropts)

        return large.opts(tools=["hover"]) * small_raster

    return hv.DynamicMap(get_elements, streams=[hv.streams.RangeX()]).opts(**ropts)
