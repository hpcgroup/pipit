import numpy as np
from bokeh.models import AdaptiveTicker, HoverTool, NumeralTickFormatter

from ._util import (
    clamp,
    format_size,
    init_vis,
    plot,
    process_tick_formatter,
    size_hover_formatter,
    size_tick_formatter,
)


def plot_comm_matrix(comm_matrix, type="size", label_threshold=16):
    """Plots heatmap of process-to-process message volume."""
    import holoviews as hv

    init_vis()

    N = comm_matrix.shape[0]
    bounds = (-0.5, -0.5, N - 0.5, N - 0.5)

    # Generate image
    image = hv.Image(np.flip(comm_matrix, 0), bounds=bounds).opts(
        # width=clamp(300 + N * 35, 400, 850),
        height=clamp(250 + N * 25, 200, 650),
        # responsive=False,
        colorbar=True,
        colorbar_position="bottom",
        cmap="YlOrRd",
        tools=[
            HoverTool(
                tooltips={
                    "Sender": "Process $x{0.}",
                    "Receiver": "Process $y{0.}",
                    "Bytes": "@image{custom}",
                },
                formatters={"@image": size_hover_formatter},
            )
        ],
        xlabel="Sender",
        ylabel="Receiver",
        xticks=AdaptiveTicker(base=2, min_interval=1),
        yticks=AdaptiveTicker(base=2, min_interval=1),
        title="Process-to-process message volume",
        yformatter=process_tick_formatter,
        xformatter=process_tick_formatter,
        xaxis="top",
        invert_yaxis=True,
        xrotation=60,
        cformatter=size_tick_formatter if type == "size" else NumeralTickFormatter(),
    )

    # Return image if label threshold not met
    if N > label_threshold:
        return plot(image)

    max_value = np.amax(comm_matrix)

    # Convert matrix from 2D array to 1D array containg (x, y, volume) values
    unrolled = []
    for i in range(N):
        for j in range(N):
            unrolled.append((i, j, comm_matrix[i, j]))

    # Generate labels
    volume_dim = hv.Dimension("volume", value_format=format_size)

    labels = hv.Labels(unrolled, vdims=volume_dim).opts(
        text_color="volume",
        color_levels=[0, max_value / 2, max_value],
        cmap=["black", "white"],
        text_font_size="9.5pt",
    )

    return plot(image * labels)
