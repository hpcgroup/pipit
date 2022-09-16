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


def css(css):
    if in_notebook():
        from IPython.display import HTML, display

        display(HTML("<style>" + css + "</style>"))


def formatter(t):
    """Converts timespan from seconds to something more readable"""
    if t < 1e-6:  # Less than 1us --> ns
        return str(round(t * 1e9)) + "ns"
    if t < 0.001:  # Less than 1ms --> us
        return str(round(t * 1e6)) + "μs"
    if t < 1:  # Less than 1s --> ms
        return str(round(t * 1000)) + "ms"
    else:
        return str(round(t, 3)) + "s"
