import holoviews as hv
import pandas as pd
from pipit.vis.util import vis_init


def comm_over_time(trace, comm_type="counts", num_bins=48):
    """Communication vs time graph (count/bytes sent for each interval)

    Args:
        trace: Trace instance being visualized
        comm_type: "counts" or "bytes"

    Returns:
        hv.Histogram: Instance of HoloViews Histogram element, which can be
            displayed in a notebook
    """
    # Initialize vis
    vis_init()
    
    # Filter by sends
    events = trace.events
    sends = events[
        (events["Event Type"] == "MpiSend") | (events["Event Type"] == "MpiISend")
    ].copy(deep=False)
    
    if("Attributes" not in sends):
        print("Message size information is not available in trace")
        return
    
    sends["Size"] = sends["Attributes"].map(lambda x: x["msg_length"])
    sends.drop(["Attributes"], axis=1)

    # Group into equal-sized bins
    sends["Bin"] = pd.cut(sends["Timestamp (ns)"], bins=num_bins, labels=False)
    df = sends.groupby("Bin").sum(numeric_only=True).reset_index()[["Bin", "Size"]]
    df = df.set_index("Bin")
    df = df.reindex(range(num_bins)).fillna(0)

    # sends.set_index("Bin")
    # return df
    # sends.fillna(0, inplace=True)

    return hv.Bars(df).opts(width=500, height=400, tools=["hover"])
