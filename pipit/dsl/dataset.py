from __future__ import annotations
from abc import ABC, abstractmethod
from pipit.dsl.event import Event

# This is the final one
class TraceData(ABC):
    @abstractmethod
    def __init__(self, data):
        self.data = data

    @abstractmethod
    def push_event(self, event: Event) -> None:
        pass

    @abstractmethod
    def flush(self) -> None:
        pass

    @abstractmethod
    def show(self) -> None:
        pass
    
    @abstractmethod
    def filter(self, condition: str) -> TraceData:
        pass

    # @abstractmethod
    # def apply(self, f):
    #     pass

    # @abstractmethod
    # def add_column(self, name, col):
    #     pass

    # @abstractmethod
    # def apply_partition(self, f):
    #     pass

    # @abstractmethod
    # def count(self):
    #     pass

    # @abstractmethod
    # def distinct(self):
    #     pass

    # @abstractmethod
    # def filter(self, f):
    #     pass

    # @abstractmethod
    # def flush(self):
    #     pass

    # @abstractmethod
    # def group_by(self, *cols):
    #     pass

    # @abstractmethod
    # def head(self, n=5):
    #     pass

    # @abstractmethod
    # def hist(self, col, bins=10):
    #     pass

    # @abstractmethod
    # def max(self, col):
    #     pass

    # @abstractmethod
    # def mean(self, col):
    #     pass

    # @abstractmethod
    # def min(self, col):
    #     pass

    # @abstractmethod
    # def ndloc(self, i):
    #     pass

    # @abstractmethod
    # def push(self, event):
    #     pass

    # @abstractmethod
    # def reduce(self, col, f):
    #     pass

    # @abstractmethod
    # def rename(self, old, new):
    #     pass

    # @abstractmethod
    # def replace(self, col, old, new):
    #     pass

    # @abstractmethod
    # def sort(self, col):
    #     pass

    # @abstractmethod
    # def sort_within_partitions(self, col):
    #     pass

    # @abstractmethod
    # def sum(self, col):
    #     pass

    # @abstractmethod
    # def tail(self, n=5):
    #     pass

def create_dataset() -> TraceData:
    from pipit.util.config import get_option

    if get_option("backend") == "pandas":
        from pipit.dsl._pandas import PandasDataset
        return PandasDataset()