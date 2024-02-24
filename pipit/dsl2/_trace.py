from __future__ import annotations
from typing import List
from pipit.dsl2.event import Event
from abc import ABC, abstractmethod


class _Trace(ABC):
    @abstractmethod
    def __init__(self, rank: int, data=None) -> None:
        pass

    def __str__(self) -> str:
        return (
            f"_Trace rank={self.rank} ({len(self)} "
            + f"event{'' if len(self) == 1 else 's'})"
        )

    def __repr__(self) -> str:
        return str(self)

    @abstractmethod
    def __len__(self) -> int:
        pass

    @property
    @abstractmethod
    def loc(self):
        pass

    @abstractmethod
    def push_event(self, event: Event) -> None:
        pass

    @abstractmethod
    def flush(self) -> None:
        pass

    @abstractmethod
    def head(self, n: int = 5) -> _Trace:
        pass

    @abstractmethod
    def tail(self, n: int = 5) -> _Trace:
        pass

    @abstractmethod
    def collect(self) -> List[Event]:
        pass

    @abstractmethod
    def show(self) -> None:
        pass

    @abstractmethod
    def filter(self, condition: str) -> _Trace:
        pass


def create_trace(backend=None, *args, **kwargs) -> _Trace:
    from pipit.util.config import get_option

    backend = backend or get_option("backend")

    if backend == "pandas":
        from pipit.dsl2._pandas import _PandasTrace

        return _PandasTrace(*args, **kwargs)
