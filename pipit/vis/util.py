import random
import holoviews as hv
from holoviews import opts

hv.extension("bokeh", logo=False)

DEFAULT_PALETTE = [
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

random.shuffle(DEFAULT_PALETTE)


def apply_bokeh_customizations(plot, _):
    plot.state.toolbar_location = "above"
    plot.state.ygrid.visible = False
    plot.state.legend.label_text_font_size = "8pt"
    plot.state.legend.spacing = 0
    plot.state.legend.location = "top"


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
    """Inject custom CSS to notebook"""
    if in_notebook():
        from IPython.display import HTML, display

        display(HTML("<style>" + css + "</style>"))


def vis_init():
    """Initialize environment for visualization"""

    # Apply css customizations, remove multiple tooltips for overlapping glyphs
    css(
        """
        .container { width:90% !important; }
        div.bk-tooltip > div.bk > div.bk:not(:last-child) {
            display:none !important;
        }
        div.bk { cursor: default !important; }
        .bk.bk-tooltip-row-label {
            color: black;
            font-weight: bold;
        }
        .bk.bk-tooltip-row-value {
            font-family: monospace;
        }
        """
    )

    defaults = dict(
        fontsize={
            "title": 10,
            "legend": 8,
        },
        hooks=[apply_bokeh_customizations],
    )

    # Apply default opts for HoloViews elements
    # See https://holoviews.org/user_guide/Applying_Customizations.html#session-specific-options # noqa: 501
    opts.defaults(
        opts.Rectangles(**defaults),
        opts.Bars(**defaults),
        opts.Image(**defaults),
        opts.Labels(**defaults),
        opts.Chord(**defaults),
        opts.Curve(**defaults),
        opts.Area(**defaults)
    )


def humanize_timedelta(ns):
    """Converts timespan from ns to something more readable"""

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


def generate_cmap(series, palette=DEFAULT_PALETTE):
    names = series.unique().tolist()

    cmap = {names[i]: palette[i] for i in range(len(names))}
    return cmap


# from pipit.vis.timeline import timeline

# timeline()
