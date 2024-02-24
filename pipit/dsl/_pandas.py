from __future__ import annotations
from pipit.dsl.dataset import TraceDataset, LocIndexer
from pipit.dsl.event import Event
from tabulate import tabulate
import pandas as pd

BUFFER_SIZE = 200


class PandasLocIndexer(LocIndexer):
    def __getitem__(self, key: tuple) -> PandasDataset | Event:
        if not isinstance(key, tuple) or len(key) != 2:
            raise ValueError("PandasLocIndexer only supports two-level indexing")

        rank, index = key
        if isinstance(rank, int):
            df = self.ds.data[rank]
            if isinstance(index, int):
                row = df.loc[index]
                rest = {k: v for k, v in row.items() if k not in ["rank", "idx"]}
                return Event(rank=rank, idx=index, **rest)
            elif isinstance(index, slice):
                return PandasDataset({rank: df.loc[index]})
            else:
                raise TypeError("Index must be an integer or a slice")
        elif isinstance(rank, slice):
            data = {r: df.loc[index] for r, df in list(self.ds.data.items())[rank]}
            return PandasDataset(data)
        else:
            raise TypeError("Rank must be an integer or a slice")


# For now assume that it is an MPI trace
class PandasDataset(TraceDataset):
    def __init__(self, data=None):
        self.data = data if data is not None else dict()
        self.backend = "pandas"
        self.buffer = dict()

    @property
    def loc(self) -> PandasLocIndexer:
        return PandasLocIndexer(self)

    def __len__(self) -> int:
        return sum(len(df) for df in self.data.values())

    def push_event(self, event: Event) -> None:
        rank = event.rank
        if rank not in self.buffer:
            self.buffer[rank] = []

        obj = event.to_dict()
        del obj["rank"]

        self.buffer[rank].append(obj)

        if len(self.buffer[rank]) >= BUFFER_SIZE:
            self.flush_rank(rank)

    def _flush_rank(self, rank: int) -> None:
        df = pd.DataFrame(self.buffer[rank])
        df.set_index("idx", inplace=True)
        if rank not in self.data:
            self.data[rank] = df
        else:
            self.data[rank] = pd.concat([self.data[rank], df])
        self.buffer[rank] = []

    def flush(self) -> None:
        for rank in self.buffer:
            self._flush_rank(rank)

    def show(self) -> None:
        if len(self):
            _ranks = list(self.data.keys())

            if len(self) > 20:
                top_df = (
                    pd.concat(
                        [self.data[rank].head(5) for rank in _ranks],
                        keys=_ranks,
                        names=["rank"],
                    )
                    .reset_index()
                    .nsmallest(5, "timestamp")
                )

                bottom_df = (
                    pd.concat(
                        [self.data[rank].tail(5) for rank in _ranks],
                        keys=_ranks,
                        names=["rank"],
                    )
                    .reset_index()
                    .nlargest(5, "timestamp")
                    .iloc[::-1]
                )

                top = top_df.to_dict(orient="records")
                middle = {k: "..." for k in top_df.columns}
                bottom = bottom_df.to_dict(orient="records")

                print(
                    tabulate(top + [middle] + bottom, headers="keys", tablefmt="psql")
                )
            else:
                df = (
                    pd.concat(
                        [self.data[rank] for rank in _ranks],
                        keys=_ranks,
                        names=["rank"],
                    )
                    .sort_values("timestamp")
                    .reset_index()
                )
                print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

        print(self.__str__())

    def filter(self, condition: str) -> PandasDataset:
        filtered_data = {rank: df.query(condition) for rank, df in self.data.items()}
        return PandasDataset(filtered_data)
