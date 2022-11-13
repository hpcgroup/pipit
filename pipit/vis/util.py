import random
import numpy as np
import pandas as pd

FUNCTION_PALETTE = [
    "rgb(138,113,152)",
    "rgb(175,112,133)",
    "rgb(127,135,225)",
    "rgb(93,81,137)",
    "rgb(116,143,119)",
    "rgb(178,214,122)",
    "rgb(87,109,147)",
    "rgb(119,155,95)",
    "rgb(114,180,160)",
    "rgb(132,85,103)",
    "rgb(157,210,150)",
    "rgb(148,94,86)",
    "rgb(164,108,138)",
    "rgb(139,191,150)",
    "rgb(110,99,145)",
    "rgb(80,129,109)",
    "rgb(125,140,149)",
    "rgb(93,124,132)",
    "rgb(140,85,140)",
    "rgb(104,163,162)",
    "rgb(132,141,178)",
    "rgb(131,105,147)",
    "rgb(135,183,98)",
    "rgb(152,134,177)",
    "rgb(141,188,141)",
    "rgb(133,160,210)",
    "rgb(126,186,148)",
    "rgb(112,198,205)",
    "rgb(180,122,195)",
    "rgb(203,144,152)",
]

DEFAULT_PALETTE = FUNCTION_PALETTE

def in_notebook():
    """Determines if we are in a notebook environment"""
    try:
        from IPython import get_ipython

        if "IPKernelApp" not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


def format_time(ns):
    """Converts timestamp/timedelta from ns to something more readable"""

    if ns < 1e3:  # Less than 1us --> ns
        return str(round(ns)) + "ns"
    if ns < 1e6:  # Less than 1ms --> us
        return str(round(ns / 1e3)) + "μs"
    if ns < 1e9:  # Less than 1s --> ms
        return str(round(ns / 1e6)) + "ms"
    else:
        return str(round(ns / 1e9, 3)) + "s"


def clamp(n, smallest, largest):
    return max(smallest, min(n, largest))


def generate_cmap(series, palette=FUNCTION_PALETTE):
    names = series.unique().tolist()

    cmap = {names[i]: palette[i] for i in range(len(names))}
    return cmap


def time_series(T=1, N=100, mu=1, sigma=0.3, S0=20):
    """Parameterized noisy time series"""
    dt = float(T) / N
    t = np.linspace(0, T, N)
    W = np.random.standard_normal(size=N)
    W = np.cumsum(W) * np.sqrt(dt)  # standard brownian motion
    X = (mu - 0.5 * sigma**2) * t + sigma * W
    S = S0 * np.exp(X)  # geometric brownian motion
    return S

def fake_time_profile(samples, num_bins, functions):
    bins = list(range(0, num_bins))

    bins_sample = np.random.choice(bins, samples)
    function_sample = np.random.choice(functions, samples)
    time = np.random.randint(1, 5, size=samples)

    df = pd.DataFrame({"bin": bins_sample, "function": function_sample, "time": time})

    return df

# from pipit.vis.timeline import timeline

# timeline()
