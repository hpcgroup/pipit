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
        trace.events = pl.from_pandas(trace.events).lazy()
        return trace
    
    def _repr_html_(self):
        return self.events.head().collect()._repr_html_()
    
    def _match_events():
        pass