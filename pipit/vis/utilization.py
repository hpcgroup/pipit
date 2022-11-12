import holoviews as hv
from holoviews import opts
import numpy as np

from .util import vis_init
from bokeh.models import HoverTool


def utilization(trace):
    """Shows the value of a certain metric over time (timeseries)"""
    vis_init()

    def time_series(T=1, N=100, mu=1, sigma=0.3, S0=20):
        """Parameterized noisy time series"""
        dt = float(T) / N
        t = np.linspace(0, T, N)
        W = np.random.standard_normal(size=N)
        W = np.cumsum(W) * np.sqrt(dt)  # standard brownian motion
        X = (mu - 0.5 * sigma**2) * t + sigma * W
        S = S0 * np.exp(X)  # geometric brownian motion
        return S

    data = time_series()

    area = hv.Area(data)
    curve = hv.Curve(data)

    return (
        (curve * area)
        .opts(
            opts.Curve(
                tools=[
                    HoverTool(
                        mode="vline", tooltips={"Time": "@x", "Utilization": "@y{0.}%"}
                    )
                ]
            ),
            opts(width=800, xlabel="Time", ylabel="% utilization"),
        )
        .relabel("% CPU utilization over time (process 0)")
    )
