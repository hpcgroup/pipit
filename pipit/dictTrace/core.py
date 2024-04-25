import pandas as pd
import numpy as np

import pipit as pp
from .util import _match_events_per_rank, _match_caller_callee_per_rank
from dask.distributed import Client


class DictTrace:
    def __init__(self, df: pd.DataFrame):
        ranks = df["Process"].unique().tolist()
        self.dfs = []

        for rank in sorted(ranks):
            self.dfs.append((rank,
                (
                    df[df["Process"] == rank]
                    .copy(deep=True)
                    .reset_index()
                    .drop("index", axis=1)
                ),
            ))

        # let's stick with dask for now for simplicity
        # this can be easily extended to ray or multiprocessing
        self.client = Client()

        # scatter the dataframes to the workers
        # this will prevent sending data for each computation
        # see https://stackoverflow.com/a/41471249
        self.dfs = self.client.scatter(self.dfs)

    def _match_events(self):
        self.dfs = self.client.map(_match_events_per_rank, self.dfs)

    def _match_caller_callee(self):
        self.dfs = self.client.map(_match_caller_callee_per_rank, self.dfs)

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
