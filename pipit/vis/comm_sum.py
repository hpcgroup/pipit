import holoviews as hv
import pandas as pd
from pipit.vis.util import vis_init


def comm_sum(trace, type="bytes", color="#30a2da"):
    """Generates bar graph of total message volume sent for each process.

    Args:
        trace: The Trace instance whose communication data is being visualized
        comm_type: Whether to compute volume by count ("counts") or by total
            message size ("bytes")
        color: CSS color for coloring bars

    Returns:
        hv.HoloMap: A HoloViews object that can be viewed in a notebook
    """

    # Initialize vis
    vis_init()

    # Filter by sends
    events = trace.events
    sends = events[events["Event Type"] == "MpiSend"]

    # Construct aggregated dataframe
    messages = pd.DataFrame()
    messages["From"] = sends["Process ID"]
    messages["Size"] = sends["Attributes"].map(lambda x: x["msg_length"])

    messages_aggr = messages.groupby("From").sum().reset_index()

    # Generate hv.Bars
    return (
        hv.Bars(messages_aggr).relabel("Communication summary")
        # .opts(width=len(sends["Process ID"].unique()) * 200, height=400)
    )
