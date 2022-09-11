# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np


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

    def calculate_inc_time(self):
        """
        An OTF2 Trace has several events that are represented using two
        rows - an "Enter" and a "Leave" that correspond to each other.
        One event (two rows) correspond to a single function call.

        This function iterates through such events and does a few things:
        1. matches each entry and exit rows pair using numerical indices
        2. determines the children of each event
        3. calculates the inclusive time for each function call
        4. determines the depth of each event in the call stack

        To reduce redundancy, the last three pieces of information listed above
        are stored only in the entry row of an event. The matching indices, by
        design, have to be stored in both so the user can find the corresponding
        "leave" row for an "enter" row of an event and vica-versa.
        """

        if "Inc Time (ns)" not in self.events.columns:
            # 4 new columns that will be added to the DataFrame
            children = [None for i in range(len(self.events))]
            matching_index = [float("nan") for i in range(len(self.events))]
            inc_time = [float("nan") for i in range(len(self.events))]
            depth = [float("nan") for i in range(len(self.events))]

            # iterate through all the Location IDs of the trace
            for location_id in set(self.events["Location ID"]):
                """
                filter the DataFrame by current Location ID so
                that the ordering of the rows make sense in the
                context of a call stack
                """
                location_df = self.events.loc[
                    (self.events["Name"] != "N/A")
                    & (self.events["Location ID"] == location_id)
                ]

                """
                Below are auxiliary lists used in the function.

                Since the DataFrame is filtered by Location ID,
                df_indices keeps trace of the DataFrame indices
                so that the metrics being calculated can be added to
                the correct row position in the DataFrame.

                The indices stack is used to keep track of the dataframe
                indices for the current callpate and calculate metrics & 
                match parents with children accordingly.
                """
                curr_depth, indices_stack, df_indices = 0, [], list(location_df.index)

                """
                copies of two columns as lists
                more efficient to iterate through these than the
                DataFrame itself (from what I've seen so far)
                """
                event_types, timestamps = list(location_df["Event Type"]), list(
                    location_df["Timestamp (ns)"]
                )

                # iterate through all events of the current Location ID
                for i in range(len(location_df)):
                    """
                    curr_df_index is the actual DataFrame index
                    that corresponds to the i'th row of location_df
                    """
                    curr_df_index = df_indices[i]

                    evt_type, timestamp = event_types[i], timestamps[i]

                    # if the row is the entry point of a function call
                    if evt_type == "Enter":
                        if curr_depth > 0:
                            """
                            if the current event is a child of another (curr depth > 0),
                            get the dataframe index of the parent event using the
                            indices stack and append the current DataFrame index to the
                            parent's children list
                            """

                            parent_df_index = indices_stack[-1]

                            if children[parent_df_index] is None:
                                """
                                create a new list of children for the parent
                                if the current event is the first child
                                being added
                                """
                                children[parent_df_index] = [curr_df_index]
                            else:
                                children[parent_df_index].append(curr_df_index)

                        """
                        The inclusive time for a function is its Leave timestamp
                        subtracted by its Enter timestamp.
                        """
                        inc_time[curr_df_index] = -timestamp

                        """
                        Whenever an entry point for a function call is encountered,
                        add the DataFrame index of the row to the indices stack
                        """
                        indices_stack.append(curr_df_index)

                        depth[curr_df_index] = curr_depth
                        curr_depth += 1  # increment the depth of the call stack

                    # if the row is the exit point of a function call
                    else:
                        """
                        get the DataFrame index of the corresponding enter row 
                        for the current leave row by popping the indices stack
                        """
                        enter_df_index = indices_stack.pop()  # corresponding enter event

                        """
                        add the matching DataFrame indices to
                        the appropriate positions in the list
                        """
                        matching_index[enter_df_index] = curr_df_index
                        matching_index[curr_df_index] = enter_df_index

                        """
                        by adding the leave timestamp, the
                        calculated time is Leave - Enter, which
                        is the inclusive time for the function call
                        """
                        inc_time[enter_df_index] += timestamp

                        curr_depth -= 1  # decrement the current depth of the call stack

            # needed because children is a list of nested lists and none objects
            children = np.array(children, dtype=object)

            """
            Create new columns of the DataFrame using
            the calculated metrics and lists above
            """
            self.events["Depth"] = depth
            self.events["Children"] = children
            self.events["Matching Index"] = matching_index
            self.events["Inc Time (ns)"] = inc_time
            
    def calculate_exc_time(self):
        """
        This function calculates the exclusive time of each function call
        by subtracting the child function times. It is meant to be called
        after using the calculate_inc_time() function.
        """

        if (
            "Children" in self.events.columns
            and "Exc Time (ns)" not in self.events.columns
        ):
            # filter the DataFrame by those rows that have children
            parents_df = self.events.loc[~self.events["Children"].isnull()]

            # DataFrame indices of the parents
            parents_indices = list(parents_df.index)

            """
            list of nested lists where each element is a list
            containing the DataFrame indices of the event's children
            """
            list_of_children = list(parents_df["Children"])

            # create exc times list as a copy of the inc times
            inc_times = list(self.events["Inc Time (ns)"])
            exc_times = list(self.events["Inc Time (ns)"])

            # iterate through the parent events
            for i in range(len(parents_indices)):
                curr_parent_idx = parents_indices[i]
                curr_children = list_of_children[i]
                # iterate through all children of the current event
                for child_index in curr_children:
                    # subtract the current child's inclusive time
                    exc_times[curr_parent_idx] -= inc_times[child_index]

            # add the list as a new column to the DataFrame
            self.events["Exc Time (ns)"] = exc_times
