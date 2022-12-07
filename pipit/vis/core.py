# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import holoviews as hv
from holoviews import opts

from bokeh.models import (
    AdaptiveTicker, 
    HoverTool
)

import random

from .util import (
    CSS,
    DEFAULT_PALETTE,
    in_notebook,
    generate_cmap,
    clamp,
    process_tick_formatter,
)

class Vis:
    """Contains data and functions related to visualization for a specific Trace instance"""

    def __init__(self, trace, server=(not in_notebook())):
        """Initialize environment for initialization.
        
        Args:
            trace (Trace): Trace instance that this Vis object is associated with
            server (bool): Whether to launch an HTTP server and display views in a browser
        """
        self.trace = trace
        self.server = server

        # Initialize HoloViews to use the Bokeh backend
        hv.extension("bokeh", logo=False, css=CSS)

        # Set some properties for easy access
        # Maybe these should be moved to the Trace class
        self.functions = trace.events[trace.events["Event Type"] == "Enter"]["Name"].unique()
        self.ranks = trace.events["Process"].unique()

        # Initialize color map for functions
        random.shuffle(DEFAULT_PALETTE)
        self.cmap = generate_cmap(self.functions, DEFAULT_PALETTE)

        # Apply default opts for HoloViews elements
        # https://holoviews.org/user_guide/Applying_Customizations.html#session-specific-options
        # https://holoviews.org/user_guide/Customizing_Plots.html#plot-hooks
        def customize_plot(plot, _):
            plot.state.toolbar_location = "above"
            plot.state.ygrid.visible = False
            if plot.state.legend:
                plot.state.legend.label_text_font_size = "8pt"
                plot.state.legend.spacing = 0
                plot.state.legend.location = "top"

        defaults = dict(
            fontsize={
                "title": 10,
                "legend": 8,
            },
            hooks=[customize_plot],
            responsive=True,
            height=300,
        )

        self.default_color = "#E5AE38"

        opts.defaults(
            opts.Area(**defaults, color=self.default_color),
            opts.Bars(**defaults, color=self.default_color),
            opts.Bivariate(**defaults),
            opts.BoxWhisker(**defaults),
            opts.Chord(**defaults),
            opts.Contours(**defaults),
            opts.Curve(**defaults, color=self.default_color),
            opts.Distribution(**defaults),
            opts.Graph(**defaults),
            opts.Histogram(**defaults, color=self.default_color),
            opts.Image(**defaults),
            opts.Labels(**defaults),
            opts.Points(**defaults),
            opts.Polygons(**defaults),
            opts.Rectangles(**defaults),
            opts.Sankey(**defaults),
            opts.Segments(**defaults),
        )


    def _view(self, element):
        """Internal function used to wrap return values in visualization functions.
        
        Launches HTTP server for element if `self.server` is true, else returns element.
        """
        if self.server:
            import panel as pn

            pn.extension(raw_css=[CSS])
            pn.panel(element).show()
            return

        return element

    def comm_heatmap(self, output="size", label_threshold=16, cmap="YlOrRd"):
        """Generates heatmap of process-to-process message volume.
        
        Uses :func:`~pipit.Trace.comm_matrix` function for calculation.

        Args:
            output (str): Whether communication volume should be determined by "size" or "count"
            label_threshold (int): Number of ranks above which labels are not displayed
            cmap (str): Name of HoloViews colormap to use
        """
        comm_matrix = self.trace.comm_matrix(output)

        num_ranks = comm_matrix.shape[0]
        bounds = (-0.5, -0.5, num_ranks - 0.5, num_ranks - 0.5)

        # Generate image
        image = hv.Image(comm_matrix, bounds=bounds).opts(
            width=clamp(300 + num_ranks * 35, 400, 850),
            height=clamp(250 + num_ranks * 25, 200, 650),
            responsive=False,
            colorbar=True,
            colorbar_position="bottom",
            cmap=cmap,
            tools=[
                HoverTool(
                    tooltips={
                        "Sender": "Process $x{0.}",
                        "Receiver": "Process $y{0.}",
                        "Count": "@image messages",
                    }
                )
            ],
            xlabel="Sender",
            ylabel="Receiver",
            xticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
            yticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
            title="Total message counts per process pair",
            yformatter=process_tick_formatter,
            xformatter=process_tick_formatter,
            xaxis="top",
            invert_yaxis=True,
            xrotation=60,
        )

        if num_ranks > label_threshold:
            return self._view(image)

        # If label threshold is met, generate labels
        max_val = np.amax(comm_matrix)
        labels = hv.Labels(image).opts(
            text_color="z",
            color_levels=[0, max_val / 2, max_val],
            cmap=["black", "white"],
            text_font_size="9.5pt",
        )

        return self._view(image * labels)


