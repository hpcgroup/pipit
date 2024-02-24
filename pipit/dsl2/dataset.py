from __future__ import annotations
from typing import Dict, List
from tabulate import tabulate
from pipit.dsl2._trace import _Trace
from pipit.dsl2.event import Event
from pipit.dsl2.util import create_trace, LocMixin


class TraceDataset(LocMixin):
    def __init__(self, traces: Dict[int, _Trace] = None) -> None:
        self.traces = traces if traces is not None else dict()

    def __str__(self) -> str:
        return (
            f"TraceDataset ({len(self.traces)}"
            + f" trace{'' if len(self.traces) == 1 else 's'},"
            + f" {len(self)} event{'' if len(self) == 1 else 's'})"
        )

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return sum(self.map_traces(lambda trace: len(trace)).values())

    @property
    def ranks(self) -> List[int]:
        return list(sorted(self.traces.keys()))

    def _locate(self, key: any) -> any:
        if isinstance(key, int):
            return TraceDataset({key: self.traces[key]})

        if isinstance(key, slice):
            start = key.start if key.start is not None else 0
            stop = key.stop if key.stop is not None else len(self.traces)
            step = key.step if key.step is not None else 1

            return TraceDataset(
                {
                    rank: trace
                    for rank, trace in self.traces.items()
                    if start <= rank < stop and (rank - start) % step == 0
                }
            )

        if isinstance(key, tuple) and len(key) == 2:
            rank, idx = key
            return self.traces[rank].loc[idx]

    def map_traces(self, f, *args, **kwargs) -> Dict[int, any]:
        results = {rank: None for rank in self.traces}

        for rank, trace in self.traces.items():
            results[rank] = f(trace, *args, **kwargs)

        return results

    def push_event(self, event: Event) -> None:
        rank = event.rank

        if rank not in self.traces:
            self.traces[rank] = create_trace(rank=rank)

        self.traces[rank].push_event(event)

    def flush(self) -> None:
        self.map_traces(lambda trace: trace.flush())

    def head(self, n: int = 5) -> TraceDataset:
        traces = self.map_traces(lambda trace: trace.head(n=n))

        events = [event for trace in traces.values() for event in trace.collect()]
        events.sort(key=lambda event: event.timestamp)

        traces = {}
        for event in events[:n]:
            rank = event.rank
            if rank not in traces:
                traces[rank] = create_trace(rank=rank)
            traces[rank].push_event(event)

        for rank in traces:
            traces[rank].flush()

        return TraceDataset(traces)

    def tail(self, n: int = 5) -> TraceDataset:
        traces = self.map_traces(lambda trace: trace.tail(n=n))

        events = [event for trace in traces.values() for event in trace.collect()]
        events.sort(key=lambda event: event.timestamp)

        traces = {}
        for event in events[-n:]:
            rank = event.rank
            if rank not in traces:
                traces[rank] = create_trace(rank=rank)
            traces[rank].push_event(event)

        for rank in traces:
            traces[rank].flush()

        return TraceDataset(traces)

    def show(self) -> None:
        if len(self):
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

    def collect(self) -> List[Event]:
        events = {rank: trace.collect() for rank, trace in self.traces.items()}
        tmp = [event for events in events.values() for event in events]
        tmp.sort(key=lambda event: event.timestamp)
        return tmp

    def filter(self, condition: str) -> TraceDataset:
        filtered_traces = self.map_traces(lambda trace: trace.filter(condition))
        return TraceDataset(filtered_traces)


# class DatasetLoc:
#     def __init__(self, ds: TraceDataset) -> None:
#         self.ds = ds

#     def __getitem__(self, key: any) -> _Trace:
#         # # Case 1: key is an integer
#         # if isinstance(key, int):
#         #     return self.ds.traces[key]

#         # # Case 2: key is a slice
#         # if isinstance(key, slice):
#         #     start = key.start if key.start is not None else 0
#         #     stop = key.stop if key.stop is not None else len(self.ds.traces)
#         #     step = key.step if key.step is not None else 1

#         #     return TraceDataset(
#         #         {
#         #             rank: trace
#         #             for rank, trace in self.ds.traces.items()
#         #             if start <= rank < stop and (rank - start) % step == 0
#         #         }
#         #     )

#         # # Case 3: key is a tuple of size != 2 => raise an error
#         # if not isinstance(key, tuple) or len(key) != 2:
#         #     raise ValueError(f"Invalid key: {key}")

#         rank, idx = key
#         return self.ds.traces[rank].loc[idx]
