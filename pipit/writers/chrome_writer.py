import pandas as pd


class ChromeWriter:
    """Exports traces to the Chrome Tracing JSON format which can be opened with Chrome
    Trace Viewer and Perfetto for analysis using these tools.

    This exports to the older Chrome Tracing JSON format which is still supported by
    Perfetto, and not the newer Perfetto binary format.

    See https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview # noqa
    """

    def __init__(self, trace, filename="trace.json"):
        self.trace = trace
        self.filename = filename

    def write(self):
        events = self.trace.events

        # Assign the fields as expected by the Chrome Tracing JSON format
        # Let's create a new dataframe to avoid modifying the original
        df = pd.DataFrame()

        # "name" represents the event name
        df["name"] = events["Name"]

        # "ph" represents event type -- also called "phase"
        # Rename Enter events to "B" (begin), Leave events to "E" (end),
        # and Instant events to "i"
        df["ph"] = events["Event Type"].replace(
            ["Enter", "Leave", "Instant"], ["B", "E", "i"]
        )

        # "ts" represents is the timestamp (in microseconds) of the event
        df["ts"] = (events["Timestamp (ns)"] / 1e3).astype(int)

        # "pid" represents the process ID for the process that the event occurs in
        df["pid"] = events["Process"]

        # "tid" represents the thread ID for the thread that the event occurs in
        if "Thread" in events.columns:
            df["tid"] = events["Thread"]

        # Put all of the additional event attributes into the "args" field
        if "Attributes" in events.columns:
            df["args"] = events["Attributes"]

        # Write the dataframe to a JSON file
        return df.to_json(path_or_buf=self.filename, orient="records")
