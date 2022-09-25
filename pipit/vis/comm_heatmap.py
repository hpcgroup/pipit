import holoviews as hv
from bokeh.models import HoverTool, PrintfTickFormatter
from pipit.util import vis_init


def comm_heatmap(trace, comm_type):
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
                <span style="font-weight: bold;">Process $x{0.} → $y{0.}</span>
            </div>
            <div>
                <span style="font-weight: bold;">Count:</span>&nbsp;
                <span style="font-family: Monaco, monospace;">@image messages</span>
            </div>
        """
    )

    # Generate heatmap image
    return hv.Image(comm_matrix, bounds=bounds).opts(
        width=max(160 + ranks * 20, 300),
        height=max(65 + ranks * 20, 200),
        colorbar=True,
        cmap="viridis",
        tools=[hover],
        xlabel="Sender",
        ylabel="Receiver",
        xticks=list(range(0, ranks)),
        yticks=list(range(0, ranks)),
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
