import holoviews as hv
from pipit.vis.util import vis_init
import numpy as np


def function_sum(trace):
    # Initialize vis
    vis_init()

    # Match enter/leave rows
    trace.calculate_inc_time()
    trace.calculate_exc_time()

    # Filter by functions
    events = trace.events
    funcs = events[events["Event"] == "Enter"][["Name", "Location ID", "Exc Time (ns)"]]

    return (
        hv.Bars(funcs, kdims=["Location ID", "Name"])
        .aggregate(function=np.sum)
        .opts(
            width=800,
            height=200,
            stacked=True,
            legend_position="right",
            invert_axes=True,
            tools=["hover"],
            default_tools=["xpan", "xwheel_zoom"],
            active_tools=["xpan", "xwheel_zoom"],
        )
    )
