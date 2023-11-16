# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import polars as pl


class Trace:
    """A trace dataset is read into an object of this type, which includes one
    or more dataframes.
    """

    def __init__(self, definitions=None, events=None):
        self.definitions = definitions
        self.events = events

    @staticmethod
    def from_parquet(path):
        """Read a trace dataset from a parquet file.

        Args:
            path (str): Path to the parquet file.

        Returns:
            Trace: A trace object.
        """
        trace = Trace()
        trace.events = pl.scan_parquet(path)
        return trace

    @staticmethod
    def from_otf2(path):
        from pipit.readers.otf2_reader import OTF2Reader

        trace = OTF2Reader(path).read()
        trace.events["Process"] = trace.events["Process"].astype("int")
        trace.events["Thread"] = trace.events["Thread"].astype("int")
        trace.events = pl.from_pandas(trace.events.reset_index()).lazy()
        return trace

    def _repr_html_(self):
        return self.events.head().collect()._repr_html_()

    def _match_events(self):
        """Add a column to the events dataframe that contains the index of the
        matching event for each event. This is used to calculate the duration
        of each event.
        """

        # Calculate the depth of each event by counting the number of
        # "Enter" events minus the number of "Leave" events that have
        # occurred so far. The "over" function is used to apply these
        # calculations per process and thread.
        self.events = self.events.with_columns(
            (
                (pl.col("Event Type") == "Enter").cumsum().shift()
                - (pl.col("Event Type") == "Leave").cumsum()
            )
            .cast(pl.Int64)
            .fill_null(0)
            .over(["Process", "Thread"])
            .alias("depth")
        )

        # Match "Enter" events with the next "Leave" event, and "Leave" events
        # with the previous "Enter" event. This is done by shifting the index
        # column by 1 for "Enter" events, and by shifting the index column by
        # -1 for "Leave" events. This shift is done per process and thread.
        self.events = self.events.with_columns(
            pl.when(pl.col("Event Type") == "Enter")
            .then(pl.col("index").shift(-1).over(["Process", "Thread", "depth"]))
            .otherwise(pl.col("index").shift(1).over(["Process", "Thread", "depth"]))
            .alias("_matching_event")
        )
