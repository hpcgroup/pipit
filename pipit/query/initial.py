# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT
from __future__ import annotations

class Dataframe:
    def __repr__(self):
        pass

    def to_dict(self) -> dict:
        pass

    def select(self, columns) -> Dataframe:
        pass

    def head(self, n=5):
        pass

    def tail(self, n=5):
        pass
    
    def filter(expr) -> Dataframe:
        """
        Filter the events based on the given expression
        """
        pass

    def groupby(columns) -> GroupBy:
        """
        Group the events based on the given columns
        """
        pass

    def get(index) -> Event:
        """
        Get the event at the given index
        """
        pass

    def apply(funcs):
        """
        Iterate over the events and apply the given functions
        """
        pass

    def exists(column):
        """
        Check if the column exists
        """
        pass

    def addColumn(name, series):
        """
        Add a new column to the events
        """
        pass

    def select(names) -> Series:
        """
        Returns a series
        """
        pass

    def addRow():
        pass

# Represents a column
class Series:
    def sum():
        """
        Sum the values of the column
        """
        pass

    def mean():
        """
        Mean of the values of the column
        """
        pass

    def max():
        """
        Max of the values of the column
        """
        pass

    def min():
        """
        Max of the values of the column
        """
        pass

    def count():
        """
        Max of the values of the column
        """
        pass

    def apply(func):
        """
        Max of the values of the column
        """
        pass

    def unique():
        """
        Max of the values of the column
        """
        pass

    def histogram():
        """
        Histogram of the values of the column
        """
        pass


class GroupBy:
    def sum(column):
        """
        Sum the values of the given column
        """
        pass

    def mean(column):
        """
        Calculate the mean of the given column
        """
        pass

    def max(column):
        """
        Calculate the max of the given column
        """
        pass

    def min(column):
        """
        Calculate the min of the given column
        """
        pass

    def count(column):
        """
        Calculate the count of the given column
        """
        pass

class Event:


# class Expr:
#     pass

# class AndExpr(Expr):
#     def __init__(self, left, right):
#         self.left = left
#         self.right = right

# class OrExpr(Expr):
#     def __init__(self, left, right):
#         self.left = left
#         self.right = right

# class NotExpr(Expr):
#     def __init__(self, expr):
#         self.expr = expr