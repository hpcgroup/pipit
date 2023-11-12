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
    def __init__(self, definitions, events):
        self.definitions = definitions
        self.events = events

    @staticmethod
    def from_parquet(filename):
        ddf = dd.read_parquet(filename)
        return Trace(events=ddf, definitions=None)
    
    @staticmethod
    def from_otf2(filename):
        from pipit.readers.otf2_reader import OTF2Reader
        trace = OTF2Reader(filename).read()
        trace.events = dd.from_pandas(trace.events, npartitions=1)
        return trace
    
    def _match_events(self):
        if "_matching_event" in self.events.columns:
            return

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

    def _match_caller_callee(self):
        if "_children" in self.events.columns:
            return
        
        if "_matching_event" not in self.events.columns:
            self._match_events()
        
        def _match_caller_callee_partition(df):
            stack = []

            df["_depth"] = "N/A"
            df["_children"] = "N/A"
            df["_parent"] = "N/A"

            eventTypes = df["Event Type"]

            for i, eventType in eventTypes.items():
                if eventType == "Enter":
                    if len(stack):
                        p = stack[-1]
                        df.at[i, "_parent"] = p

                        if df.at[p, "_children"] == "N/A":
                            df.at[p, "_children"] = [i]
                        else:
                            df.at[p, "_children"].append(i)

                    df.at[i, "_depth"] = len(stack)
                    stack.append(i)
                elif eventType == "Leave":
                    stack.pop()

            return df

        self.events = self.events.map_partitions(_match_caller_callee_partition)