import pandas as pd
import numpy as np

import pipit as pp
from .util import _match_events_per_rank, _match_caller_callee_per_rank


class DictTrace:
    def __init__(self, df: pd.DataFrame):
        ranks = df["Process"].unique().tolist()
        self.dfs = dict()

        for rank in ranks:
            self.dfs[rank] = (
                df[df["Process"] == rank]
                .copy(deep=True)
                .reset_index()
                .drop("index", axis=1)
            )

        match pp.get_option("backend"):
            case "dask":
                from dask.distributed import Client

                self.client = Client()
            case "ray":
                import ray

                ray.init(ignore_reinit_error=True)
            case "multiprocessing":
                import concurrent.futures

                self.client = concurrent.futures.ProcessPoolExecutor()

    def _match_events(self):
        match pp.get_option("backend"):
            case "dask":
                futures = self.client.map(_match_events_per_rank.remote, self.dfs.items())
                results = self.client.gather(futures)
                self.dfs = {rank: df for rank, df in results}
            case "ray":
                import ray

                futures = [
                    _match_events_per_rank.remote((rank, df))
                    for rank, df in self.dfs.items()
                ]
                results = ray.get(futures)
                self.dfs = {rank: df for rank, df in results}
            case "multiprocessing":
                self.executor.map(_match_caller_callee_per_rank, self.dfs.items())

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
