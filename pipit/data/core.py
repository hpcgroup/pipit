from abc import ABC, abstractmethod

# This is based on HoloViews Dataset
# contains everything from query/core.py and query/initial.py
# https://holoviews.org/reference_manual/holoviews.core.data.html#holoviews.core.data.Dataset

class TraceData(ABC):
    """Abstract base class representing a trace dataset."""

    @abstractmethod
    def add_attribute(self, attribute, value, index):
        pass

    @abstractmethod
    def apply(self, func):
        pass

    @abstractmethod
    def array(self, attributes):
        pass

    @abstractmethod
    def clone(self):
        pass

    @abstractmethod
    def columns(self, attributes):
        pass

    @abstractmethod
    def attribute_values(self, attribute, expanded=True):
        pass

    @abstractmethod
    def attributes(self, selection):
        pass

    @abstractmethod
    def flush(self):
        pass

    @abstractmethod
    def get_attribute(self, attribute):
        pass

    @abstractmethod
    def get_attribute_index(self, attribute):
        pass

    @abstractmethod
    def get_attribute_type(self, attribute):
        pass

    @abstractmethod
    def groupby(self, attributes, container_type):
        pass

    @abstractmethod
    def head(self, n):
        pass

    @abstractmethod
    def hist(self, attribute, num_bins, bin_range):
        pass

    @abstractmethod
    def max(self, attributes):
        pass

    @abstractmethod
    def mean(self, attributes):
        pass

    @abstractmethod
    def min(self, attributes):
        pass

    @abstractmethod
    def ndloc(self, *args):
        pass

    @abstractmethod
    def options(self, *args):
        pass

    @abstractmethod
    def persist(self):
        pass

    @abstractmethod
    def push(self, event):
        pass

    @abstractmethod
    def range(self, attribute):
        pass

    @abstractmethod
    def reduce(self, attributes, function, spreadfn):
        pass

    @abstractmethod
    def select(self, selection_expr):
        pass

    @abstractmethod
    def sort(self, by, reverse):
        pass

    @abstractmethod
    def sum(self, attributes):
        pass

    @abstractmethod
    def tail(self, n):
        pass

    @abstractmethod
    def __len__(self):
        pass