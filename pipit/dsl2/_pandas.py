from __future__ import annotations
from typing import List
from tabulate import tabulate
import pandas as pd
from pipit.dsl.event import Event
from pipit.dsl._trace import _Trace


class _PandasTrace(_Trace):
    def __init__(self, rank: int, data=None) -> None:
        self.data = data if data is not None else pd.DataFrame()
        self.rank = rank
        self.buffer = []

    def __len__(self) -> int:
        return len(self.data)

    @property
    def loc(self):
        pass

    def push_event(self, event: Event) -> None:
        obj = event.to_dict()
        del obj["rank"]

        self.buffer.append(obj)

        if len(self.buffer) >= 200:
            self.flush()

    def flush(self) -> None:
        if len(self.buffer) == 0:
            return

        df = pd.DataFrame(self.buffer)
        df.set_index("idx", inplace=True)
        self.data = pd.concat([self.data, df])
        self.buffer = []

    def head(self, n: int = 5) -> _PandasTrace:
        df = self.data.head(n=n)
        return _Trace(df, rank=self.rank)

    def tail(self, n: int = 5) -> _PandasTrace:
        df = self.data.tail(n=n)
        return _Trace(df, rank=self.rank)

    def collect(self) -> List[Event]:
        records = self.data.reset_index().to_dict(orient="records")
        events = [Event(rank=self.rank, **record) for record in records]
        return events

    def show(self) -> None:
        if len(self):
            if len(self) > 20:
                top_df = self.head().data.reset_index()
                bottom_df = self.tail().data.reset_index()

                top = top_df.to_dict(orient="records")
                middle = {k: "..." for k in top_df.columns}
                bottom = bottom_df.to_dict(orient="records")

                print(
                    tabulate(top + [middle] + bottom, headers="keys", tablefmt="psql")
                )
            else:
                print(
                    tabulate(
                        self.data, headers="keys", tablefmt="psql", showindex=False
                    )
                )

        print(self.__str__())

    def filter(self, condition: str) -> _PandasTrace:
        return _Trace(self.data.query(condition), rank=self.rank)
