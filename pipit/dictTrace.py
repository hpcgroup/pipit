import pandas as pd
import numpy as np
from dask.distributed import Client

def _match_events_per_rank(args):
    rank, df = args
    matching_events = np.full(len(df), np.nan)

    stack = []
    idx = df.index.tolist()
    event_type = df["Event Type"].tolist()

    for i in range(len(idx)):
        if event_type[i] == "Enter":
            stack.append(idx[i])
        elif event_type[i] == "Leave":
            matching_idx = stack.pop()
            matching_events[idx[i]] = matching_idx
            matching_events[matching_idx] = idx[i]

    df["_matching_event"] = matching_events
    return rank, df

class DictTrace:
    def __init__(self, df: pd.DataFrame):
        ranks = df["Process"].unique().tolist()
        self.dfs = dict()

        for rank in ranks:
            self.dfs[rank] = df[df["Process"] == rank].copy(deep=True).reset_index().drop("index", axis=1)

        self.client = Client()
            
    def _match_events(self):
        futures = self.client.map(_match_events_per_rank, self.dfs.items())
        self.dfs = dict(self.client.gather(futures))

    def _match_caller_callee(self):
        def _match_caller_callee_per_rank(rank):
            df = self.dfs[rank]

            depth = np.full(len(df), np.nan)
            parent = np.full(len(df), np.nan)

            stack = []
            curr_depth = 0

            idx = df.index.tolist()
            event_type = df["Event Type"].tolist()

            for i in range(len(idx)):
                if event_type[i] == "Enter":
                    depth[idx[i]] = curr_depth
                    if curr_depth > 0:
                        parent[idx[i]] = stack[-1]
                    curr_depth += 1
                    stack.append(idx[i])
                elif event_type[i] == "Leave":
                    stack.pop()
                    curr_depth -= 1

            df["_depth"] = depth
            df["_parent"] = parent
            self.dfs[rank] = df.astype({"_depth": "Int32", "_parent": "Int32"})

        futures = self.client.map(_match_caller_callee_per_rank, self.dfs.keys())
        self.client.gather(futures)

    def calc_inc_metrics(self):
        pass

    def calc_exc_metrics(self):
        pass

    def comm_matrix(self):
        pass

    def message_histogram(self):
        pass

    def __del__(self):
        self.client.close()