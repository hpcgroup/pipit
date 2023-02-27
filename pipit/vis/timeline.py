import datashader as ds
import holoviews as hv
import pandas as pd
from holoviews import opts
from holoviews.operation.datashader import datashade
from ._util import getProcessTickFormatter, getTimeTickFormatter
import pipit as pp

hv.extension("bokeh")


def hook(plot, _):
    plot.handles["yaxis"].minor_tick_line_color = None


def plot_timeline(trace):
    trace._pair_enter_leave()
    trace._gen_calling_relationships()
    trace.calc_inc_time()

    func = trace.events[trace.events["Event Type"] == "Enter"]

    colors = pp.config["vis"]["colors"]
    color_key = {
        cat: tuple(
            int(x) for x in colors[i].replace("rgb(", "").replace(")", "").split(",")
        )
        for i, cat in enumerate(func["Name"].unique())
    }
    color_key_ds = {
        key: tuple(int(x * 0.9) for x in value) for key, value in color_key.items()
    }

    rects_df = pd.DataFrame()
    rects_df["x0"] = func["Timestamp (ns)"]
    rects_df["y0"] = func["Process"].astype("float") - 0.5
    rects_df["x1"] = func["Matching Timestamp"]
    rects_df["y1"] = func["Process"].astype("float") + 0.5
    rects_df["name"] = func["Name"].astype("category")

    scatter_df = pd.DataFrame()
    scatter_df["x"] = func["Timestamp (ns)"]
    scatter_df["y"] = func["Process"].astype("float")
    scatter_df["name"] = func["Name"].astype("category")

    rects_opts = opts.Rectangles(
        fill_color="name",
        line_width=0.2,
        line_color="black",
        cmap=color_key,
        show_legend=False,
    )

    copts = dict(
        responsive=True,
        height=min(700, max(160, len(func["Process"].unique()) * 40)),
        default_tools=["xpan", "xwheel_zoom"],
        active_tools=["xpan", "xwheel_zoom"],
        xaxis="top",
        labelled=[],
        xformatter=getTimeTickFormatter(),
        yformatter=getProcessTickFormatter(),
        yticks=len(func["Process"].unique()),
        show_grid=True,
        gridstyle=dict(ygrid_line_color=None),
        invert_yaxis=True,
        hooks=[hook],
        title="Timeline",
    )

    rects = hv.Rectangles(rects_df, vdims=["name"]).opts(rects_opts).opts(**copts)
    # scatter = hv.Points(scatter_df, vdims=["name"]).opts(jitter=0.5, **copts)

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
        ).opts(**copts)

        return large.opts(tools=["hover"]) * small_raster

    return hv.DynamicMap(get_elements, streams=[hv.streams.RangeX()]).opts(**copts)
