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

    def convert(self):
        events = self.trace.events

        df = pd.DataFrame()
        df["name"] = events["Name"]
        df["ph"] = events["Event Type"].replace(
            ["Enter", "Leave", "Instant"], ["B", "E", "i"]
        )
        df["ts"] = (events["Timestamp (ns)"] / 1e3).astype(int)
        df["pid"] = events["Process"]

        if "Thread" in events.columns:
            df["tid"] = events["Thread"]

        if "Attributes" in events.columns:
            df["args"] = events["Attributes"]

        return df

    def write(self):
        df = self.convert()
        return df.to_json(path_or_buf=self.filename, orient="records")
