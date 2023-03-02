import datashader as ds
import holoviews as hv
from holoviews.operation.datashader import datashade
from ._util import getProcessTickFormatter, getTimeTickFormatter
from bokeh.models import Grid, FixedTicker
import numpy as np
import pipit as pp

hv.extension("bokeh")
hv.renderer("bokeh").webgl = True

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
    height=500,
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

    global added
    added = False
    inst = trace.events[trace.events["Event Type"] == "Instant"].copy(deep=False)
    inst["y"] = inst["Process"].astype("float")

    func = trace.events[trace.events["Event Type"] == "Enter"].copy(deep=False)
    func["ys0"] = func["Process"].astype("float")
    func["ys1"] = func["Process"].astype("float")
    func["yr0"] = func["Process"].astype("float") - 0.5
    func["yr1"] = func["Process"].astype("float") + 0.5
    func["Name"] = func["Name"].astype("category")
    func["y"] = func["Process"].astype("float")
    func["Inc Time"] = func["Matching Timestamp"] - func["Timestamp (ns)"]

    func = func.sort_values(by=["Inc Time"], ascending=False)

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

    num_ranks = int(trace.events["Process"].astype("float").max() + 1)

    hline = hv.Overlay(
        [
            hv.HLine(y=n + 0.5).opts(line_color="lightgray", line_width=0.7)
            for n in range(num_ranks)
        ]
    )

    def get_elements(x_range, scale, width, height, x, y):
        low, high = (
            (0, trace.events["Timestamp (ns)"].max()) if x_range is None else x_range
        )
        scale, width, height = (
            (1.0, 800, 400)
            if None in (scale, width, height)
            else (scale, width, height)
        )
        buffer = (high - low) * 0.25

        # Draw the 5000 largest functions, rasterize the rest!
        in_range = func[
            ~(
                (func["Matching Timestamp"] < low - buffer)
                | (func["Timestamp (ns)"] > high + buffer)
            )
        ]
        large = in_range.head(5000)

        comp = hv.Overlay()
        comp *= (
            hv.Rectangles(
                large,
                kdims=["Timestamp (ns)", "yr0", "Matching Timestamp", "yr1"],
                vdims=["Name", "y", "Inc Time"],
            )
            .opts(line_width=0.1, line_color="black", fill_color="Name", cmap=color_key)
            .opts(**ropts)
        )

        if len(in_range) > 1000:
            small = in_range.tail(len(in_range) - 1000)
            raster = datashade(
                hv.Points(small, kdims=["Timestamp (ns)", "y"], vdims=["Name"]),
                dynamic=False,
                min_alpha=200,
                width=int(width),
                height=num_ranks,
                x_range=x_range,
                y_range=(-0.5, num_ranks - 0.5),
                aggregator=ds.count_cat("Name"),
                color_key=color_key_ds,
            )
            comp *= raster.opts(**ropts)

        return comp.opts(**ropts)

    return (
        hv.DynamicMap(
            get_elements,
            streams=[
                hv.streams.RangeX(),
                hv.streams.PlotSize(),
                hv.streams.PointerXY(),
            ],
        ).opts(**ropts)
        * hline
    )
