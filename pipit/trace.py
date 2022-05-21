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

    def time_per_func_occurrence(self, metric="exc"):
        """
        Arguments:
        metric - a string that can be either "exc" or "inc"

        Returns:
        A dictionary where the keys are the names of all the events in the trace
        and the values are lists containing the times that every individual function
        occurrence of that event took. Depending on the metric paramter, the times
        will either be inclusive or exclusive.

        The dictionary's values can be used to create a scatterplot of the times of
        all of a function's occurrences, calculate imbalance, etc.
        """

        if metric == "exc":
            col_name = "Exc Time (ns)"
        elif metric == "inc":
            col_name = "Inc Time (ns)"
        else:
            print('Input a valid metric - either "exc" or "inc".')

        return (
            self.events.loc[self.events["Event Type"] == "Enter"]
            .groupby("Name", observed=True)[col_name]
            .apply(list)
            .to_dict()
        )

    def flat_profile(self, metric="both"):
        """
        Arguments:
        metric - a string that can be either "both", "exc", or "inc"

        Returns:
        A Pandas DataFrame where each row corresponds to a function
        and it will have the total summed up inclusive or exclusive time
        for that function as columns. Depending on the metric parameter,
        there will either be an exclusive column, inclusive column, or both.

        Note:
        Filtering by rank, location id, event names, etc has not been added.
        Perhaps a query language similar to Hatchet can be utilized.
        Only very basic error handling has been added to some of the functions.
        These are areas to touch up on throughout the program.
        """

        if metric == "both":
            columns = ["Exc Time (ns)", "Inc Time (ns)"]
        elif metric == "exc":
            columns = "Exc Time (ns)"
        elif metric == "inc":
            columns = "Inc Time (ns)"
        else:
            print('Input a valid metric - either "both", "exc", or "inc".')

        return (
            self.events.loc[self.events["Event Type"] == "Enter"]
            .groupby("Name", observed=True)[columns]
            .sum()
        )
