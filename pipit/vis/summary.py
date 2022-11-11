import holoviews as hv
from pipit.vis.util import (
    DEFAULT_PALETTE,
    generate_cmap,
    vis_init,
)
from bokeh.models import HoverTool
import numpy as np


def summary(trace, type="exc", palette=DEFAULT_PALETTE):
    """Generates bar graph of total time spent by function for each process.

    Args:
        trace: Trace instance whose events are being visualized
        type: Whether to aggregate by inclusive ("inc") or exclusive ("exc")
            function durations
        cmap: HoloViews cmap

    Returns:
        hv.HoloMap: A HoloViews object that can be viewed in a notebook
    """

    # Initialize vis
    vis_init()

    # Calculate inc/exc times
    trace.calc_inc_time()
    trace.calc_exc_time()

    # Filter by functions
    events = trace.events
    funcs = events[events["Event Type"] == "Entry"][["Name", "Process ID", "Exc Time"]]

    return (
        hv.Bars(funcs, kdims=["Process ID", "Name"])
        .aggregate(function=np.sum)
        .opts(
            width=800,
            height=len(events["Process ID"].unique()) * 90,
            stacked=True,
            cmap=generate_cmap(funcs["Name"], palette),
            legend_position="right",
            invert_axes=True,
            tools=[HoverTool(
                tooltips={
                    "Name": "@{Name}",
                    "Total time": "@{Exc_Time}",
                    "Process ID": "@{Process_ID}",
                },
                point_policy="follow_mouse"
            )],
            default_tools=["xpan", "xwheel_zoom"],
            active_tools=["xpan", "xwheel_zoom"],
            line_width=0.2,
            line_color="white",
        )
        .relabel("Total exc time per function per process")
    )
