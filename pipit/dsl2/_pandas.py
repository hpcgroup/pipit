from __future__ import annotations
from typing import List, Generator
from tabulate import tabulate
import pandas as pd
from pipit.dsl2.event import Event
from pipit.dsl2._trace import _Trace
from pipit.dsl2.reduce import DictLike


class _PandasTrace(_Trace):
    """
    Pandas-based implementation of the _Trace class, where the trace
    of a single rank is contained in a pandas DataFrame.
    """

    def __init__(self, rank: int, data=None) -> None:
        self.data = data if data is not None else pd.DataFrame()
        self.rank = rank
        self.buffer = []

    def count(self) -> int:
        return len(self.data)

    def _locate(self, key: any) -> Event | _PandasTrace:
        # case 1: key is an integer
        if isinstance(key, int):
            row = self.data.loc[key]
            return Event(rank=self.rank, idx=row.name, **row.to_dict())

        # case 2: key is a slice
        if isinstance(key, slice):
            return _PandasTrace(rank=self.rank, data=self.data.loc[key])

    def push_event(self, event: Event) -> None:
        # remove rank from the event since it's already known
        obj = event.to_dict()
        del obj["rank"]

        self.buffer.append(obj)

        # flush buffer if it's full
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
        df = self.data.head(n)
        return _PandasTrace(rank=self.rank, data=df)

    def tail(self, n: int = 5) -> _PandasTrace:
        df = self.data.tail(n)
        return _PandasTrace(rank=self.rank, data=df)

    def collect(self) -> List[Event]:
        records = self.data.reset_index().to_dict(orient="records")

        # add the rank back since it's not in the DataFrame
        events = [Event(rank=self.rank, **record) for record in records]
        return events

    def show(self) -> None:
        # maybe we can make this common for all backends
        if self.count():
            if self.count() > 20:
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
                        self.data.reset_index(),
                        headers="keys",
                        tablefmt="psql",
                        showindex=False,
                    )
                )

        print(self.__str__())

    def filter(self, condition: str) -> _PandasTrace:
        df = self.data.query(condition)
        return _PandasTrace(rank=self.rank, data=df)

    def map_events(self, func, columns=None, *args, **kwargs) -> DictLike:
        # TODO: Investigate how to make this more efficient

        # Simplest implementation:

        # series = self.data.apply(func, axis=1, *args, **kwargs)
        # return DictLike(data=series.to_dict(), key_label="idx", value_label="result")

        # From copilot:

        # The apply function in pandas is known to be slow because it applies
        # a function to each row or column of the DataFrame individually. This
        # can be quite slow for large DataFrames.

        # If func can be vectorized (i.e., applied to entire arrays at once),
        # you can use a vectorized operation instead of apply to make this
        # function more efficient.

        # However, without knowing the exact nature of func, it's hard to
        # provide a specific solution. If func is a simple operation that
        # can be vectorized, you could replace the apply call with the
        # appropriate pandas or numpy function.

        # If func cannot be vectorized, you might be able to make this
        # function more efficient by using raw numpy arrays instead of
        # pandas DataFrames. The apply function in pandas has to do a lot
        # of type checking and other overhead that can slow it down, so
        # using raw numpy arrays can be faster.

        # Here's a general example of how you might do this:

        # def map_events(self, func, attributes=None, *args, **kwargs) -> DictLike:
        #     np_arrays = {col: self.data[col].values for col in self.data.columns}
        #     results = {idx: func(*row, *args, **kwargs) for idx, row in
        #                enumerate(zip(*np_arrays.values()))}
        #     return DictLike(data=results, key_label="idx", value_label="result")

        # In this version, zip(*np_arrays.values()) is used to transpose
        # the dictionary of numpy arrays into a list of tuples, where each
        # tuple represents a row in the DataFrame. This allows us to apply
        # func to each row in a single loop, which is faster than using apply.
        # The results are then converted back into a dictionary to create a
        # DictLike object.

        # Please note that this is a general solution and might not work for
        # all cases. Depending on the nature of func and your specific use
        # case, you might need to adjust this code to fit your needs.

        columns = columns if columns is not None else self.data.columns

        # Check that the columns are valid
        for col in columns:
            if col not in self.data.columns:
                raise ValueError(f"Column '{col}' not found in the DataFrame.")

        np_arrays = {col: self.data[col].values for col in self.data.columns}

        results = {
            idx: func(
                Event(
                    rank=self.rank,
                    idx=idx,
                    **{col: np_arrays[col][idx] for col in columns},
                ),
                *args,
                **kwargs,
            )
            for idx in range(len(self.data))
        }
        return DictLike(data=results, key_label="idx", value_label="result")

    def iter_events(self, columns=None) -> Generator[Event, None, None]:
        columns = columns if columns is not None else self.data.columns

        # Check that the columns are valid
        for col in columns:
            if col not in self.data.columns:
                raise ValueError(f"Column '{col}' not found in the DataFrame.")

        np_arrays = {col: self.data[col].values for col in columns}

        for i in range(len(self.data)):
            # yield {col: np_arrays[col][i] for col in columns}
            yield Event(
                rank=self.rank, idx=i, **{col: np_arrays[col][i] for col in columns}
            )

    def add_column(self, name: str, values: any) -> None:
        if len(values) != len(self.data):
            raise ValueError(
                f"Length of values ({len(values)}) does not match"
                + f" the length of the trace ({len(self.data)})"
            )

        self.data[name] = values
