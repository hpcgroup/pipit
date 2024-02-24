from __future__ import annotations
import pandas as pd
from pipit.dsl.event import Event
from tabulate import tabulate


# TODO: rename to TraceDataset
class Dataset:
    def __init__(self, traces=None) -> None:
        self.traces = traces if traces is not None else dict()

    def __str__(self) -> str:
        return f"Dataset ({len(self)} event{'' if len(self) == 1 else 's'})"

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return sum(len(trace) for trace in self.traces.values())

    @property
    def loc(self):
        pass

    def push_event(self, event) -> None:
        rank = event.rank

        if rank not in self.traces:
            self.traces[rank] = _Trace(rank=rank)

        self.traces[rank].push_event(event)

    def flush(self) -> None:
        for rank in self.traces:
            self.traces[rank].flush()

    def head(self, n=5) -> Dataset:
        traces = {rank: trace.head(n=n) for rank, trace in self.traces.items()}
        events = [event for trace in traces.values() for event in trace.collect()]

        events.sort(key=lambda event: event.timestamp)

        traces = {}
        for event in events[:n]:
            rank = event.rank
            if rank not in traces:
                traces[rank] = _Trace(rank=rank)
            traces[rank].push_event(event)

        for rank in traces:
            traces[rank].flush()

        return Dataset(traces)

    def tail(self, n=5) -> Dataset:
        traces = {rank: trace.tail(n=n) for rank, trace in self.traces.items()}
        events = [event for trace in traces.values() for event in trace.collect()]

        events.sort(key=lambda event: event.timestamp)

        traces = {}
        for event in events[-n:]:
            rank = event.rank
            if rank not in traces:
                traces[rank] = _Trace(rank=rank)
            traces[rank].push_event(event)

        for rank in traces:
            traces[rank].flush()

        return Dataset(traces)

    def show(self) -> None:
        if len:
            if len(self) > 20:
                top = [e.to_dict() for e in self.head().collect()]
                middle = {k: "..." for k in top[0].keys()}
                bottom = [e.to_dict() for e in self.tail().collect()]

                print(
                    tabulate(top + [middle] + bottom, headers="keys", tablefmt="psql")
                )
            else:
                print(
                    tabulate(
                        [e.to_dict() for e in self.collect()],
                        headers="keys",
                        tablefmt="psql",
                    )
                )

        print(self.__str__())

    def collect(self):
        events = {rank: trace.collect() for rank, trace in self.traces.items()}
        tmp = [event for events in events.values() for event in events]
        tmp.sort(key=lambda event: event.timestamp)
        return tmp

    def filter(self, condition) -> Dataset:
        filtered_traces = {
            rank: trace.filter(condition) for rank, trace in self.traces.items()
        }
        return Dataset(filtered_traces)

    def map_ranks(self, f) -> None:
        pass


class _Trace:
    def __init__(self, data=None, rank=0) -> None:
        self.data = data if data is not None else pd.DataFrame()
        self.rank = rank
        self.buffer = []

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return f"_Trace ({len(self)} event{'' if len(self) == 1 else 's'})"

    def __repr__(self) -> str:
        return str(self)

    @property
    def loc(self):
        pass

    def push_event(self, event) -> None:
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

    def head(self, n=5) -> _Trace:
        df = self.data.head(n=n)
        return _Trace(df, rank=self.rank)

    def tail(self, n=5) -> _Trace:
        df = self.data.tail(n=n)
        return _Trace(df, rank=self.rank)

    def collect(self):
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

    def filter(self, condition) -> _Trace:
        return _Trace(self.data.query(condition), rank=self.rank)
