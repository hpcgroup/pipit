from pipit import Trace
from .util import parse_time
import pandas as pd


class LocIndexer:
    """Extends Pandas .loc functionality for Trace objects."""

    def __init__(self, trace):
        self.trace = trace

        self.process = ColIndexer(trace, "Process")
        self.name = ColIndexer(trace, "Name")
        self.timestamp = ColIndexer(trace, "Timestamp (ns)", is_time=True)
        self.thread = ColIndexer(trace, "Thread")
        self.type = ColIndexer(trace, "Event Type")

    def __getitem__(self, key):
        item = self.trace.events.loc[key]

        if type(item) == pd.DataFrame:
            return Trace(self.trace.definitions, self.trace.events.loc[key])
        else:
            return item


class ColIndexer:
    """Extends Pandas .loc functionality for Trace objects."""

    def __init__(self, trace, column, is_time=False):
        self.trace = trace
        self.column = column
        self.is_time = is_time

    def __getitem__(self, key):
        df = self.trace.events

        if self.is_time:
            key = parse_time(key)

        if type(key) == tuple:
            df = df[df[self.column].isin(key)]
        elif type(key) == slice:
            self.trace._match_events()

            partial_df = df[(df[self.column] >= key.start) & (df[self.column] < key.stop)]

            if (self.column == "Timestamp (ns)"):
                matching_rows = df.loc[partial_df['_matching_event'].dropna().tolist()]
                partial_df = pd.concat([partial_df, matching_rows]).sort_index()

            df = partial_df
        else:
            df = df[df[self.column] == key]

        return Trace(
            self.trace.definitions,
            df,
        )
