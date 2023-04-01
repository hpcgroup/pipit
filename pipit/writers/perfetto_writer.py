import pandas as pd


class PerfettoWriter:
    """Exports traces to Chrome Tracing JSON format which can be opened with Perfetto"""

    def __init__(self, trace, file_name):
        self.trace = trace
        self.file_name = file_name

    def convert(self):
        events = self.trace.events

        df = pd.DataFrame()
        df["name"] = events["Name"]
        df["ph"] = events["Event Type"].replace(
            ["Enter", "Leave", "Instant"], ["B", "E", "i"]
        )
        df["pid"] = events["Process"]
        df["tid"] = events["Thread"]
        df["ts"] = (events["Timestamp (ns)"] / 1e3).astype(int)

        return df

    def write(self):
        df = self.convert()
        df.to_json(self.file_name, orient="records")
