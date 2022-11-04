import holoviews as hv
import pandas as pd
from pipit.vis.util import vis_init
from holoviews import opts


def comm_graph(trace):
    """Communication graph visualization."""
    # Initialize vis
    vis_init()

    # Filter by sends/recvs
    events = trace.events
    sends = events[events["Event Type"] == "MpiSend"]

    messages = pd.DataFrame()
    messages["From"] = sends["Process ID"]
    
    if("Attributes" not in sends):
        print("Sender/receiver information is not available in trace")
        return
    
    messages["To"] = sends["Attributes"].map(lambda x: x["receiver"])
    messages["Size"] = sends["Attributes"].map(lambda x: x["msg_length"])

    messages_aggr = messages.groupby(["From", "To"]).sum().reset_index()

    return hv.Chord(messages_aggr).opts(
        opts.Chord(
            cmap="Category20",
            edge_cmap="Category20",
            edge_color=hv.dim("From").str(),
            node_color=hv.dim("index").str(),
        )
    )
