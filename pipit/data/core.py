from abc import 

# This is based on HoloViews Dataset
# contains everything from query/core.py and query/initial.py
# https://holoviews.org/reference_manual/holoviews.core.data.html#holoviews.core.data.Dataset
class Dataset:
    def add_dimension(dimension, dim_val, dim_index):
        pass

    def aggregate(dimensions, function, spreadfn):
        pass

    def apply(funcs):
        pass

    def array(dimensions):
        pass

    def clone():
        pass

    def columns(dimensions):
        pass

    def dimension_values(dimension, expanded=True):
        pass

    def dimensions(selection):
        pass

    def flush():
        pass

    def get_dimension(dimension):
        pass

    def get_dimension_index(dimension):
        pass

    def get_dimension_type(dimension):
        pass

    def groupby(dimensions, container_type):
        pass

    def head(n):
        pass

    def hist(dimension, num_bins, bin_range):
        pass

    def max(dimensions):
        pass

    def mean(dimensions):
        pass

    def min(dimensions):
        pass

    def ndloc(*args):
        pass

    def options(*args):
        pass

    def persist():
        pass

    def push(event):
        pass

    def range(dimension):
        pass

    def reduce(dimensions, function, spreadfn):
        pass

    def select(selection_expr):
        pass

    def sort(by, reverse):
        pass

    def sum(dimensions):
        pass

    def tail(n):
        pass

    def __len__():
        pass