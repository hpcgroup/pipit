import holoviews as hv
import pandas as pd
from pipit.vis.util import vis_init
from holoviews import opts


def comm_graph(trace, cmap="Category20"):
    """Generates a network graph visualization for communication, with nodes
        representing processes and edges representing messages.

    Args:
        trace: Trace instance whose communication is being visualized
        cmap: Name of HoloViews colormap to use
            (see https://holoviews.org/user_guide/Colormaps.html)

    Returns:
        hv.HoloMap: A HoloViews object that can be viewed in a notebook
    """

    # Initialize vis
    vis_init()

    # Filter by sends/recvs
    events = trace.events
    sends = events[events["Event Type"] == "MpiSend"]

    messages = pd.DataFrame()
    messages["From"] = sends["Process ID"]

    if "Attributes" not in sends:
        print("Sender/receiver information is not available in trace")
        return

    messages["To"] = sends["Attributes"].map(lambda x: x["receiver"])
    # messages["Size"] = sends["Attributes"].map(lambda x: x["msg_length"])
    # Todo: use message size and normalize
    # Todo: check if message size is available
    messages["Size"] = 1

    messages_aggr = messages.groupby(["From", "To"]).sum().reset_index()
    messages_aggr = messages_aggr[messages_aggr["From"] != messages_aggr["To"]]

    return (
        hv.Chord(messages_aggr)
        .opts(
            opts.Chord(
                cmap=cmap,
                edge_cmap=cmap,
                # edge_color=hv.dim("To").str(),
                node_color=hv.dim("index").str(),
                # labels="Label"
            )
        )
        .relabel("Communication graph")
    )
