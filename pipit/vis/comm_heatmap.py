import holoviews as hv
from bokeh.models import HoverTool, PrintfTickFormatter, AdaptiveTicker
from pipit.vis.util import clamp, vis_init
import numpy as np


def comm_heatmap(trace, comm_type="counts", label_threshold=16, cmap="blues"):
    """Generates interactive plot of comm_matrix"""

    # Initialize vis
    vis_init()

    # Calculate communication matrix for given comm_type
    comm_matrix = trace.comm_matrix(comm_type)

    # Determine heatmap image bounds
    ranks = comm_matrix.shape[0]
    bounds = (-0.5, -0.5, ranks - 0.5, ranks - 0.5)

    # Custom tooltip
    hover = HoverTool(
        tooltips="""
            <div>
                <span style="font-weight: bold;">Process IDs:</span>&nbsp;
                <span style="font-family: Monaco, monospace;">$x{0.} → $y{0.}</span>
            </div>
            <div>
                <span style="font-weight: bold;">Count:</span>&nbsp;
                <span style="font-family: Monaco, monospace;">@image messages</span>
            </div>
        """
    )

    # Generate heatmap image
    image = hv.Image(comm_matrix, bounds=bounds).opts(
        width=clamp(160 + ranks * 35, 300, 850),
        height=clamp(65 + ranks * 25, 200, 650),
        colorbar=True,
        cmap=cmap,
        tools=[hover],
        xlabel="Sender",
        ylabel="Receiver",
        xticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
        yticks=AdaptiveTicker(base=2, min_interval=1, max_interval=None),
        fontsize={
            "title": 10,
            "legend": 8,
        },
        padding=0,
        title="Communication Heatmap",
        yformatter=PrintfTickFormatter(format="Process %d"),
        xformatter=PrintfTickFormatter(format="Process %d"),
        xaxis="top",
        invert_yaxis=True,
        xrotation=60,
    )

    if ranks > label_threshold:
        return image

    # Generate labels
    max_val = np.amax(comm_matrix)
    labels = hv.Labels(image).opts(
        text_color="z",
        color_levels=[0, max_val / 2, max_val],
        cmap=["black", "white"],
        text_font_size="8pt",
    )

    return image * labels
