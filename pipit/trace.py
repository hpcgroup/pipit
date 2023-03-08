# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
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
    def from_otf2(dirname, num_processes=None):
        """Read an OTF2 trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.otf2_reader import OTF2Reader

        return OTF2Reader(dirname, num_processes).read()

    @staticmethod
    def from_hpctoolkit(dirname):
        """Read an HPCToolkit trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.hpctoolkit_reader import HPCToolkitReader

        return HPCToolkitReader(dirname).read()

    @staticmethod
    def from_projections(dirname):
        """Read a Projections trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.projections_reader import ProjectionsReader

        return ProjectionsReader(dirname).read()

    @staticmethod
    def from_nsight(filename):
        """Read an Nsight trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.nsight_reader import NsightReader

        return NsightReader(filename).read()

    def _match_events(self):
        """Matches corresponding enter/leave events and adds two columns to the
        dataframe: _matching_event and _matching_timestamp
        """

        if "_matching_event" not in self.events.columns:
            matching_events = [float("nan")] * len(self.events)
            matching_times = [float("nan")] * len(self.events)

            # only pairing enter and leave rows
            enter_leave_df = self.events.loc[
                self.events["Event Type"].isin(["Enter", "Leave"])
            ]

            for process in set(enter_leave_df["Process"]):
                curr_process_df = enter_leave_df.loc[
                    enter_leave_df["Process"] == process
                ]
                for thread in set(curr_process_df["Thread"]):
                    # filter by both process and thread
                    filtered_df = curr_process_df.loc[
                        curr_process_df["Thread"] == thread
                    ]

                    stack = []

                    # Note: The reason that we are creating lists that are
                    # copies of the dataframe columns below and iterating over
                    # those instead of using pandas iterrows is due to an
                    # observed improvement in performance when using lists.

                    event_types = list(filtered_df["Event Type"])
                    df_indices, timestamps = list(filtered_df.index), list(
                        filtered_df["Timestamp (ns)"]
                    )

                    # Iterate through all events of filtered DataFrame
                    for i in range(len(filtered_df)):
                        curr_df_index, curr_timestamp, evt_type = (
                            df_indices[i],
                            timestamps[i],
                            event_types[i],
                        )

                        if evt_type == "Enter":
                            # Add current dataframe index and timestamp to stack
                            stack.append((curr_df_index, curr_timestamp))
                        else:
                            # Pop corresponding enter event's dataframe index
                            # and timestamp
                            enter_df_index, enter_timestamp = stack.pop()

                            # Fill in the lists with the matching values
                            matching_events[enter_df_index] = curr_df_index
                            matching_events[curr_df_index] = enter_df_index

                            matching_times[enter_df_index] = curr_timestamp
                            matching_times[curr_df_index] = enter_timestamp

            self.events["_matching_event"] = matching_events
            self.events["_matching_timestamp"] = matching_times

            self.events = self.events.astype({"_matching_event": "Int32"})

    def _match_caller_callee(self):
        """Matches callers (parents) to callees (children) and adds three
        columns to the dataframe:
        _depth, _parent, and _children

        _depth is level in the call tree starting at 0.
        _parent is the dataframe index of a row's parent event.
        _children is a list of dataframe indices of a row's children events.
        """

        if "_children" not in self.events.columns:
            children = [None] * len(self.events)
            depth, parent = [float("nan")] * len(self.events), [float("nan")] * len(
                self.events
            )

            # only using enter and leave rows
            # to determine calling relationships
            enter_leave_df = self.events.loc[
                self.events["Event Type"].isin(["Enter", "Leave"])
            ]

            for process in set(enter_leave_df["Process"]):
                curr_process_df = enter_leave_df.loc[
                    enter_leave_df["Process"] == process
                ]
                for thread in set(curr_process_df["Thread"]):
                    # filter by both process and thread
                    filtered_df = curr_process_df.loc[
                        curr_process_df["Thread"] == thread
                    ]

                    # Depth is the level in the
                    # Call Tree starting from 0
                    curr_depth = 0

                    stack = []
                    df_indices, event_types = list(filtered_df.index), list(
                        filtered_df["Event Type"]
                    )

                    # loop through the events of the filtered dataframe
                    for i in range(len(filtered_df)):
                        curr_df_index, evt_type = df_indices[i], event_types[i]

                        if evt_type == "Enter":
                            if (
                                curr_depth > 0
                            ):  # if event is a child of some other event
                                parent_df_index = stack[-1]

                                if children[parent_df_index] is None:
                                    # create a new list of children for the
                                    # parent if the current event is the first
                                    # child being added
                                    children[parent_df_index] = [curr_df_index]
                                else:
                                    children[parent_df_index].append(curr_df_index)

                                parent[curr_df_index] = parent_df_index

                            depth[curr_df_index] = curr_depth
                            curr_depth += 1

                            # add enter dataframe index to stack
                            stack.append(curr_df_index)
                        else:
                            # pop event off stack once matching leave found
                            # Note: depth, parent, and children for a leave row
                            # can be found using the matching index that
                            # corresponds to the enter row
                            stack.pop()

                            curr_depth -= 1

            self.events["_depth"], self.events["_parent"], self.events["_children"] = (
                depth,
                parent,
                children,
            )

            self.events = self.events.astype({"_depth": "Int32", "_parent": "Int32"})

            self.events = self.events.astype(
                {"_depth": "category", "_parent": "category"}
            )

    def calc_inc_time(self):
        # Adds "time.inc" column
        if "time.inc" not in self.events.columns:
            if "_matching_timestamp" not in self.events.columns:
                self._match_events()

            # Uses matching timestamp to calculate the inclusive time
            self.events.loc[self.events["Event Type"] == "Enter", "time.inc"] = (
                self.events["_matching_timestamp"] - self.events["Timestamp (ns)"]
            )

    def calc_exc_time(self):
        if "time.exc" not in self.events.columns:
            if "time.inc" not in self.events.columns:
                self.calc_inc_time()
            if "_children" not in self.events.columns:
                self._match_caller_callee()

            # start out with exc times being a copy of inc times
            exc_times = self.events["time.inc"].to_list()
            inc_times = self.events["time.inc"].to_list()

            # Filter to events that have children
            filtered_df = self.events.loc[self.events["_children"].notnull()]
            parent_df_indices, children = (
                list(filtered_df.index),
                filtered_df["_children"].to_list(),
            )

            # Iterate through the events that are parents
            for i in range(len(filtered_df)):
                curr_parent_idx, curr_children = parent_df_indices[i], children[i]
                for child_idx in curr_children:
                    # Subtract child's inclusive time to update parent's exclusive time
                    exc_times[curr_parent_idx] -= inc_times[child_idx]

            self.events["time.exc"] = exc_times

    def comm_matrix(self, output="size"):
        """
        Communication Matrix for Peer-to-Peer (P2P) MPI messages

        Arguments:

        1) output -
        string to choose whether the communication volume should be measured
        by bytes transferred between two processes or the number of messages
        sent (two choices - "size" or "count")

        Returns:
        Creates three lists - sender ranks, receiver ranks, and message volume.
        All of these lists are the length of the number of messages sent in the trace.
        It then loops through these lists containing individual message pairs
        and volume for those messages and updates the comm matrix.

        Finally, a 2D Numpy Array that represents the communication matrix for all P2P
        messages of the given trace is returned.

        Note:
        The first dimension of the returned 2d array
        is senders and the second dimension is receivers
        ex) comm_matrix[sender_rank][receiver_rank]
        """

        # get the list of ranks/processes
        # (mpi messages are sent between processes)
        ranks = set(self.events["Process"])

        # create a 2d numpy array that will be returned
        # at the end of the function
        communication_matrix = np.zeros(shape=(len(ranks), len(ranks)))

        # filter the dataframe by MPI Send and Isend events
        sender_dataframe = self.events.loc[
            self.events["Name"].isin(["MpiSend", "MpiIsend"]),
            ["Process", "Attributes"],
        ]

        # get the mpi ranks of all the sender processes
        # the length of the list is the total number of messages sent
        sender_ranks = sender_dataframe["Process"].to_list()

        # get the corresponding mpi ranks of the receivers
        # the length of the list is the total number of messages sent
        receiver_ranks = (
            sender_dataframe["Attributes"]
            .apply(lambda attrDict: attrDict["receiver"])
            .to_list()
        )

        # the length of the message_volume list created below
        # is the total number of messages sent

        # number of bytes communicated for each message sent
        if output == "size":
            # (1 communication is a single row in the sender dataframe)
            message_volume = (
                sender_dataframe["Attributes"]
                .apply(lambda attrDict: attrDict["msg_length"])
                .to_list()
            )
        elif output == "count":
            # 1 message between the pairs of processes
            # for each row in the sender dataframe
            message_volume = np.full(len(sender_dataframe), 1)

        for i in range(len(sender_ranks)):
            """
            loops through all the communication events and adds the
            message volume to the corresponding entry of the 2d array
            using the sender and receiver ranks
            """
            communication_matrix[sender_ranks[i], receiver_ranks[i]] += message_volume[
                i
            ]

        return communication_matrix

    def message_histogram(self, bins=20, **kwargs):
        """Generates histogram of message frequency by size."""

        # Filter by send events
        messages = self.events[self.events["Name"].isin(["MpiSend", "MpiIsend"])]

        # Get message sizes
        sizes = messages["Attributes"].map(lambda x: x["msg_length"])

        return np.histogram(sizes, bins=bins, **kwargs)

    def flat_profile(self, metric=["Exc Time"], groupby_column="Name"):
        """
        Arguments:
        metric - a string or list of strings containing the metrics to be aggregated
        groupby_column - a string or list containing the columns to be grouped by

        Returns:
        A Pandas DataFrame that will have the aggregated metrics
        for the grouped by columns.
        """

        if "Inc Time" in metric and "Inc Time" not in self.events.columns:
            self.calc_inc_time()
        if "Exc Time" in metric and "Exc Time" not in self.events.columns:
            self.calc_exc_time()

        return (
            self.events.loc[self.events["Event Type"] == "Enter"]
            .groupby(groupby_column, observed=True)[metric]
            .sum()
        )

    def metric_per_func_occurrence(self, metric="Exc Time", groupby_column="Name"):
        """
        Arguments:
        metric - a string that can be either "Exc Time" or "Inc Time"
        groupby_column - a string or list of strings containing the column(s)
                         to be grouped by

        Returns:
        A dictionary where the keys are the set of groupby column values
        (ex: function names) and the values are lists containing the metrics
        for every individual function occurrence corresponding to the key
        (ex: exclusive times).
        """

        if metric == "Exc Time" and "Exc Time" not in self.events.columns:
            self.calc_exc_time()
        elif metric == "Inc Time" and "Inc Time" not in self.events.columns:
            self.calc_inc_time()

        return (
            self.events.loc[self.events["Event Type"] == "Enter"]
            .groupby(groupby_column, observed=True)[metric]
            .apply(list)
            .to_dict()
        )
