import holoviews as hv
from holoviews import opts
import pandas as pd
import numpy as np

from .util import vis_init, generate_cmap
from bokeh.models import HoverTool

def time_profile(trace, num_bins=16):
    vis_init()

    samples = 100

    bins = list(range(0, 64))
    functions = trace.events["Name"].unique()

    bins_sample = np.random.choice(bins, samples)
    gender_sample = np.random.choice(functions, samples)
    count = np.random.randint(1, 5, size=samples)

    df = pd.DataFrame({'bins': bins_sample, 'Gender': gender_sample, 'Count': count})

    bars = hv.Bars(df, kdims=['bins', 'Gender']).sort().aggregate(function=np.sum)
    return bars.opts(
        stacked=True,
        width=800,
        xlabel="Time interval",
        ylabel="Time contribution (ms)",
        cmap=generate_cmap(functions),
        legend_position="right",
        line_width=0.2,
        line_color="white",
        xformatter=None
    ).relabel("Exclusive time per function per time interval")