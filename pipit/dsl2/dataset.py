from __future__ import annotations
from typing import List
from tabulate import tabulate
from pipit.dsl2.event import Event
from pipit.dsl2.util import create_trace, LocMixin
from pipit.dsl2.reduce import DictLike


class TraceDataset(LocMixin):
    """
    A collection of traces of different ranks, represented by
    a dictionary that maps each rank to its trace (_Trace). Also
    contains methods for building, querying, and manipulating
    events in the dataset.

    This may become an abstract class in the future, which is
    implemented by different backends. However, we would still
    need to have an abstract _Trace class, to expose to the
    user the `map_traces` method, which should work on an abstract
    _Trace instead of, say, a pandas DataFrame. Thus, a backend
    would have to implement both the TraceDataset and _Trace
    classes. Currently, we simplify this by having TraceDataset
    be a concrete class that uses an abstract _Trace class.
    """

    def __init__(self, traces: DictLike = None) -> None:
        self.traces = traces if traces is not None else DictLike()

    def __str__(self) -> str:
        return (
            f"TraceDataset ({len(self.traces)}"
            + f" trace{'' if len(self.traces) == 1 else 's'},"
            + f" {self.count()} event{'' if self.count() == 1 else 's'})"
        )

    def __repr__(self) -> str:
        return str(self)

    def count(self) -> int:
        """
        Returns the number of events in the dataset, across all ranks.
        """
        return self.map_traces(lambda trace: trace.count()).reduce("sum")

    @property
    def ranks(self) -> List[int]:
        return list(sorted(self.traces.keys()))

    def _locate(self, key: any) -> Event | TraceDataset:
        """
        Select events by rank and index.
        """
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

    def map_traces(self, f, *args, **kwargs) -> DictLike:
        """
        Applies a function to each trace in the dataset.
        """
        results = DictLike(
            data={rank: None for rank in self.traces},
            key_label="rank",
            value_label="result",
        )

        for rank, trace in self.traces.items():
            results[rank] = f(trace, *args, **kwargs)

        return results

    def push_event(self, event: Event) -> None:
        """
        Adds an event to the appropriate rank's trace.

        Assumes that the event is in the correct order by timestamp.
        """
        rank = event.rank

        if rank not in self.traces:
            self.traces[rank] = create_trace(rank=rank)

        self.traces[rank].push_event(event)

    def flush(self) -> None:
        """
        Flushes the buffers of all traces in the dataset.

        This is used to ensure that all events are written to the
        traces in the dataset.
        """
        self.map_traces(lambda trace: trace.flush())

    def head(self, n: int = 5) -> TraceDataset:
        """
        Returns the first n events in the dataset across all ranks,
        sorted by timestamp.
        """
        events = self.map_traces(lambda trace: trace.head(n).collect()).reduce("concat")
        events.sort(key=lambda event: event.timestamp)

        traces = DictLike()
        for event in events[:n]:
            rank = event.rank
            if rank not in traces:
                traces[rank] = create_trace(rank=rank)
            traces[rank].push_event(event)

        for rank in traces:
            traces[rank].flush()

        return TraceDataset(traces)

    def tail(self, n: int = 5) -> TraceDataset:
        """
        Returns the last n events in the dataset across all ranks,
        sorted by timestamp.
        """
        events = self.map_traces(lambda trace: trace.tail(n).collect()).reduce("concat")
        events.sort(key=lambda event: event.timestamp)

        traces = DictLike()
        for event in events[-n:]:
            rank = event.rank
            if rank not in traces:
                traces[rank] = create_trace(rank=rank)
            traces[rank].push_event(event)

        for rank in traces:
            traces[rank].flush()

        return TraceDataset(traces)

    def show(self) -> None:
        """
        Prints a preview of the dataset across all ranks.
        """
        if self.count():
            if self.count() > 20:
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
        """
        Returns all events in the dataset as a list of Event objects.

        This may be both compute and memory intensive for large datasets,
        especially if the data is columnar and needs to be reassembled.
        """
        events = self.map_traces(lambda trace: trace.collect()).reduce("concat")
        events.sort(key=lambda event: event.timestamp)
        return events

    def filter(self, condition: str) -> TraceDataset:
        """
        Filters the dataset using the given condition.
        """
        filtered_traces = self.map_traces(lambda trace: trace.filter(condition))
        return TraceDataset(filtered_traces)
