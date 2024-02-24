from __future__ import annotations
from typing import Dict, List
from tabulate import tabulate
from pipit.dsl2._trace import _Trace
from pipit.dsl.event import Event


# TODO: rename to TraceDataset
class Dataset:
    def __init__(self, traces: Dict[int, _Trace] = None) -> None:
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

    def push_event(self, event: Event) -> None:
        rank = event.rank

        if rank not in self.traces:
            self.traces[rank] = _Trace(rank=rank)

        self.traces[rank].push_event(event)

    def flush(self) -> None:
        for rank in self.traces:
            self.traces[rank].flush()

    def head(self, n: int = 5) -> Dataset:
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

    def tail(self, n: int = 5) -> Dataset:
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

    def filter(self, condition: str) -> Dataset:
        filtered_traces = {
            rank: trace.filter(condition) for rank, trace in self.traces.items()
        }
        return Dataset(filtered_traces)

    def map_ranks(self, f) -> None:
        pass
