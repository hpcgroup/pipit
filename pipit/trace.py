# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT


class Trace:
    """A trace dataset is read into an object of this type, which includes one
    or more dataframes.
    """

    def __init__(self, definitions, events):
        """Create a new Trace object."""
        self.definitions = definitions
        self.events = events

    @staticmethod
    def from_otf2(dirname):
        """Read an OTF2 trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.otf2_reader import OTF2Reader

        return OTF2Reader(dirname).read()

    @staticmethod
    def from_hpctoolkit(dirname):
        """Read an HPCToolkit trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.hpctoolkit_reader import HPCToolkitReader

        return HPCToolkitReader(dirname).read()

    """
    Note: Relies on Standardizing and Inc/Exc PRs to be merged first!
    """

    def time_per_func_occurrence(self, metric="Exc Time", groupby_column="Name"):
        """
        Arguments:
        metric - a string that can be either "Exc Time" or "Inc Time"
        groupby_column - a string containing the column to be grouped by

        Returns:
        A dictionary where the keys are the names of all the events in the trace
        and the values are lists containing the times that every individual function
        occurrence of that event took. Depending on the metric parameter, the times
        will either be inclusive or exclusive.

        The dictionary's values can be used to create a scatterplot of the times of
        all of a function's occurrences, calculate imbalance, etc.
        """

        if metric == "Exc Time" and "Exc Time" not in self.events.columns:
            self.calc_exc_time()  # once inc/exc pr is merged
        elif metric == "Inc Time" and "Inc Time" not in self.events.columns:
            self.calc_inc_time()  # once inc/exc pr is merged

        return (
            self.events.loc[self.events["Event Type"] == "Entry"]
            .groupby(groupby_column, observed=True)[metric]
            .apply(list)
            .to_dict()
        )

    def flat_profile(self, metric=["Inc Time", "Exc Time"], groupby_column="Name"):
        """
        Arguments:
        metric - a string or list of strings containing the metrics to be aggregated
        groupby_column - a string or list containing the columns to be grouped by

        Returns:
        A Pandas DataFrame that will have the aggregated metrics
        for the grouped by columns.

        Note:
        Filtering by process id, event names, etc has not been added.
        Perhaps a query language similar to Hatchet can be utilized.
        There should also be some error handling.
        These are areas to touch up on throughout the repo.
        """

        if "Inc Time" in metric and "Inc Time" not in self.events.columns:
            self.calc_inc_time()  # once inc/exc pr is merged
        if "Exc Time" in metric and "Exc Time" not in self.events.columns:
            self.calc_exc_time()  # once inc/exc pr is merged

        return (
            self.events.loc[self.events["Event Type"] == "Entry"]
            .groupby(groupby_column, observed=True)[metric]
            .sum()
        )
