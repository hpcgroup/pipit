# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import dask.dataframe as dd


class Trace:
    """A trace dataset is read into an object of this type, which includes one
    or more dataframes.
    """
    def __init__(self, events, definitions):
        self.definitions = definitions
        self.events = events

    @staticmethod
    def from_parquet(filename):
        ddf = dd.read_parquet(filename)
        return Trace(events=ddf, definitions=None)
    
    def _match_events(self):
        def _match_events_partition(df):
            stack = []
            df["_matching_event"] = "N/A"

            eventTypes = df["Event Type"]

            for i, eventType in eventTypes.items():
                if eventType == "Enter":
                    stack.append(i)
                elif eventType == "Leave":
                    m = stack.pop()
                    df.at[m, "_matching_event"] = i
                    df.at[i, "_matching_event"] = m

            return df
        
        self.events = self.events.map_partitions(_match_events_partition)