import holoviews as hv
from bokeh.models import HoverTool
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
            <b>Count: @image</b></br>
            <em>Process $x{0.} → $y{0.}</em>
        """
    )

    # Generate heatmap image
    return hv.Image(comm_matrix, bounds=bounds).opts(
        width=max(160 + ranks * 20, 250),
        height=max(65 + ranks * 20, 150),
        colorbar=True,
        cmap="viridis",
        tools=[hover],
        xlabel="Sender ID",
        ylabel="Receiver ID",
        xticks=list(range(0, ranks)),
        yticks=list(range(0, ranks)),
        fontsize={
            "title": 10,
            "legend": 8,
        },
        padding=0,
        title="Communication Heatmap",
    )
