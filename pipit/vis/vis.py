import holoviews as hv
from holoviews import opts
from .util import in_notebook, DEFAULT_PALETTE, generate_cmap
import random

hv.extension("bokeh", logo=False)


class Vis:
    """Contains visualization data and functions for a Trace"""

    def __init__(self, trace):
        """Initialize environment for visualization"""
        self.trace = trace

        # Apply css customizations, remove multiple tooltips for overlapping glyphs
        if in_notebook():
            from IPython.display import HTML, display

            display(
                HTML(
                    """
                <style>
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
                        padding-left: 3px;
                    }
                </style>
                """
                )
            )

        # Initialize color map
        random.shuffle(DEFAULT_PALETTE)
        funcs = trace.events[trace.events["Event Type"] == "Entry"]["Name"].unique()
        self.cmap = generate_cmap(funcs, DEFAULT_PALETTE)

        # Apply default opts for HoloViews elements
        # See https://holoviews.org/user_guide/Applying_Customizations.html#session-specific-options # noqa: 501
        def customize_plot(plot, _):
            plot.state.toolbar_location = "above"
            plot.state.ygrid.visible = False
            plot.state.legend.label_text_font_size = "8pt"
            plot.state.legend.spacing = 0
            plot.state.legend.location = "top"

        defaults = dict(
            fontsize={
                "title": 10,
                "legend": 8,
            },
            hooks=[customize_plot],
        )

        opts.defaults(
            opts.Area(**defaults),
            opts.Bars(**defaults),
            opts.Bivariate(**defaults),
            opts.BoxWhisker(**defaults),
            opts.Chord(**defaults),
            opts.Contours(**defaults),
            opts.Curve(**defaults),
            opts.Distribution(**defaults),
            opts.Graph(**defaults),
            opts.Image(**defaults),
            opts.Labels(**defaults),
            opts.Points(**defaults),
            opts.Polygons(**defaults),
            opts.Rectangles(**defaults),
            opts.Sankey(**defaults),
            opts.Segments(**defaults),
        )

    def timeline() -> hv.HoloMap:
        """Draw interactive timeline of events"""
        pass

    def utilization():
        pass

    def time_profile():
        pass

    def summary():
        pass

    def histogram():
        pass

    def comm_heatmap():
        pass

    def tree():
        pass
