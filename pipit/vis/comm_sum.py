import holoviews as hv
import pandas as pd
from pipit.vis.util import vis_init


def comm_sum(trace):
    """Communication summary for each process"""
    # Initialize vis
    vis_init()

    # Filter by sends
    events = trace.events
    sends = events[events["Event Type"] == "MpiSend"]

    messages = pd.DataFrame()
    messages["From"] = sends["Process ID"]
    messages["Size"] = sends["Attributes"].map(lambda x: x["msg_length"])

    messages_aggr = messages.groupby("From").sum().reset_index()

    # Draw bars
    return (
        hv.Bars(messages_aggr)
        .relabel("Total message size sent by process")
        .opts(width=800, height=400)
    )
