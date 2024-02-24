from __future__ import annotations
from abc import ABC, abstractmethod
from pipit.dsl.event import Event


class LocIndexer(ABC):
    def __init__(self, ds):
        self.ds = ds

    def __getitem__(self, key) -> Event | TraceDataset:
        pass


# This is the final one
class TraceDataset(ABC):
    @abstractmethod
    def __init__(self, data=None):
        """
        TODO: provide option for different partition column than "rank"
        see https://docs.dask.org/en/latest/generated/dask.dataframe.DataFrame.shuffle.html
        or maybe provide a "shuffle" method to achieve more parallelism than just partitioning by rank
        streams are a list of execution locations that are being traced.
        streams can be nested, for example:
        ["process"], [("process", "thread")], [("process", "thread"), "gpu"]
        """
        self.data = data
        self.backend = None

    def __str__(self) -> str:
        return f":TraceDataset   ({len(self)} events)"

    def __repr__(self):
        return str(self)
    
    @abstractmethod
    def __len__(self) -> int:
        pass

    @property
    @abstractmethod
    def loc(self) -> LocIndexer:
        pass

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
    def filter(self, condition: str) -> TraceDataset:
        pass

    # @abstractmethod
    # def map_ranks(self, f) -> TraceDataset:
    #     # maybe we can generalize this to map_threads for OpenMP, etc.
    #     pass

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
    # def distinct(self):
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


def create_dataset(backend=None, *args, **kwargs) -> TraceDataset:
    from pipit.util.config import get_option

    backend = backend or get_option("backend")

    if backend == "pandas":
        from pipit.dsl._pandas import PandasDataset

        return PandasDataset(*args, **kwargs)
