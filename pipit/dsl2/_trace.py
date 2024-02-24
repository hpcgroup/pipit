from __future__ import annotations
from typing import List
from pipit.dsl2.event import Event
from abc import ABC, abstractmethod
from pipit.dsl2.util import LocMixin
from pipit.dsl2.reduce import Reducible


class _Trace(LocMixin, ABC):
    """
    Represents a trace of events from a single rank.
    """

    @abstractmethod
    def __init__(self, rank: int, data=None) -> None:
        pass

    def __str__(self) -> str:
        return (
            f"_Trace (rank={self.rank}, {self.count()} "
            + f"event{'' if self.count() == 1 else 's'})"
        )

    def __repr__(self) -> str:
        return str(self)

    @abstractmethod
    def count(self) -> int:
        """
        Returns the number of events in the trace.
        May be changed to `count()` in the future.
        """
        pass

    @abstractmethod
    def _locate(self, key: any) -> Event | _Trace:
        """
        Select events by index.
        """
        pass

    @abstractmethod
    def push_event(self, event: Event) -> None:
        """
        Adds an event to the buffer. If the buffer is full, the events
        are flushed to the trace.
        """
        pass

    @abstractmethod
    def flush(self) -> None:
        """
        Flushes the buffer to the trace.
        """
        pass

    @abstractmethod
    def head(self, n: int = 5) -> _Trace:
        """
        Returns the first n events (by timestamp) in the trace.
        """
        pass

    @abstractmethod
    def tail(self, n: int = 5) -> _Trace:
        """
        Returns the last n events (by timestamp) in the trace.
        """
        pass

    @abstractmethod
    def collect(self) -> List[Event]:
        """
        Returns all events in the trace as a list of Event objects.

        This may be both compute and memory intensive for large datasets,
        especially if the data is columnar and needs to be reassembled.
        """
        pass

    @abstractmethod
    def show(self) -> None:
        """
        Prints a preview of the trace.
        """
        pass

    @abstractmethod
    def filter(self, condition: str) -> _Trace:
        """
        Filters the trace using the given condition.
        """
        pass

    @abstractmethod
    def map_events(self, f, *args, **kwargs) -> Reducible:
        """
        Applies a function to each event in the trace.
        """
        pass
