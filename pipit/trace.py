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

        # list of numeric columns which we can calculate inc/exc metrics with
        self.numeric_cols = list(
            self.events.select_dtypes(include=[np.number]).columns.values
        )

        # will store columns names for inc/exc metrics
        self.inc_metrics = []
        self.exc_metrics = []

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

        return HPCToolkitReader(dirname).get_trace()

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

            # list of processes and/or threads to iterate over
            if "Thread" in self.events.columns:
                exec_locations = set(zip(self.events["Process"], self.events["Thread"]))
                has_thread = True
            else:
                exec_locations = set(self.events["Process"])
                has_thread = False

            for curr_loc in exec_locations:
                # only filter by thread if the trace has a thread column
                if has_thread:
                    curr_process, curr_thread = curr_loc
                    filtered_df = enter_leave_df.loc[
                        (enter_leave_df["Process"] == curr_process)
                        & (enter_leave_df["Thread"] == curr_thread)
                    ]
                else:
                    filtered_df = enter_leave_df.loc[
                        (enter_leave_df["Process"] == curr_loc)
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
        """Matches callers (parents) to callees (children) and adds two
        columns to the dataframe:
        _parent, and _children
        _parent is the dataframe index of a row's parent event.
        _children is a list of dataframe indices of a row's children events.
        """

        if "_children" not in self.events.columns:
            children = [None] * len(self.events)
            parent = [float("nan")] * len(self.events)

            # only use enter and leave rows
            # to determine calling relationships
            enter_leave_df = self.events.loc[
                self.events["Event Type"].isin(["Enter", "Leave"])
            ]

            # list of processes and/or threads to iterate over
            if "Thread" in self.events.columns:
                exec_locations = set(zip(self.events["Process"], self.events["Thread"]))
                has_thread = True
            else:
                exec_locations = set(self.events["Process"])
                has_thread = False

            for curr_loc in exec_locations:
                # only filter by thread if the trace has a thread column
                if has_thread:
                    curr_process, curr_thread = curr_loc
                    filtered_df = enter_leave_df.loc[
                        (enter_leave_df["Process"] == curr_process)
                        & (enter_leave_df["Thread"] == curr_thread)
                    ]
                else:
                    filtered_df = enter_leave_df.loc[
                        (enter_leave_df["Process"] == curr_loc)
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
                        if curr_depth > 0:  # if event is a child of some other event
                            parent_df_index = stack[-1]

                            if children[parent_df_index] is None:
                                # create a new list of children for the
                                # parent if the current event is the first
                                # child being added
                                children[parent_df_index] = [curr_df_index]
                            else:
                                children[parent_df_index].append(curr_df_index)

                            parent[curr_df_index] = parent_df_index

                        curr_depth += 1

                        # add enter dataframe index to stack
                        stack.append(curr_df_index)
                    else:
                        # pop event off stack once matching leave found
                        # Note: parent, and children for a leave row
                        # can be found using the matching index that
                        # corresponds to the enter row
                        stack.pop()

                        curr_depth -= 1

            self.events["_parent"], self.events["_children"] = (
                parent,
                children,
            )

            self.events = self.events.astype({"_parent": "Int32"})

            self.events = self.events.astype({"_parent": "category"})

    def calc_inc_metrics(self, columns=None):
        # if no columns are specified by the user, then we calculate
        # inclusive metrics for all the numeric columns in the trace
        columns = self.numeric_cols if columns is None else columns

        # pair enter and leave rows
        if "_matching_event" not in self.events.columns:
            self._match_events()

        enter_df = self.events.loc[self.events["Event Type"] == "Enter"]

        # calculate inclusive metric for each column specified
        for col in columns:
            # name of column for this inclusive metric
            metric_col_name = ("time" if col == "Timestamp (ns)" else col) + ".inc"

            if metric_col_name not in self.events.columns:
                # calculate the inclusive metric by subtracting
                # the values at the enter rows from the values
                # at the corresponding leave rows
                self.events.loc[
                    self.events["Event Type"] == "Enter", metric_col_name
                ] = (
                    self.events[col][enter_df["_matching_event"]].values
                    - enter_df[col].values
                )

                self.inc_metrics.append(metric_col_name)

    def calc_exc_metrics(self, columns=None):
        # calculate exc metrics for all numeric columns if not specified
        columns = self.numeric_cols if columns is None else columns

        # match caller and callee rows
        if "_children" not in self.events.columns:
            self._match_caller_callee()

        # exclusive metrics only change for rows that have children
        filtered_df = self.events.loc[self.events["_children"].notnull()]
        parent_df_indices, children = (
            list(filtered_df.index),
            filtered_df["_children"].to_list(),
        )

        for col in columns:
            # get the corresponding inclusive column name for this metric
            inc_col_name = ("time" if col == "Timestamp (ns)" else col) + ".inc"
            if inc_col_name not in self.events.columns:
                self.calc_inc_metrics([col])

            # name of column for this exclusive metric
            metric_col_name = ("time" if col == "Timestamp (ns)" else col) + ".exc"

            if metric_col_name not in self.events.columns:
                # exc metric starts out as a copy of the inc metric values
                exc_values = self.events[inc_col_name].to_list()
                inc_values = self.events[inc_col_name].to_list()

                for i in range(len(filtered_df)):
                    curr_parent_idx, curr_children = parent_df_indices[i], children[i]
                    for child_idx in curr_children:
                        # subtract each child's inclusive metric from the total
                        # to calculate the exclusive metric for the parent
                        exc_values[curr_parent_idx] -= inc_values[child_idx]

                self.events[metric_col_name] = exc_values
                self.exc_metrics.append(metric_col_name)

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

    def flat_profile(self, metrics=None, groupby_column="Name"):
        """
        Arguments:
        metric - a string or list of strings containing the metrics to be aggregated
        groupby_column - a string or list containing the columns to be grouped by
        Returns:
        A Pandas DataFrame that will have the aggregated metrics
        for the grouped by columns.
        """

        metrics = self.inc_metrics + self.exc_metrics if metrics is None else metrics

        # This first groups by both the process and the specified groupby
        # column (like name). It then sums up the metrics for each combination
        # of the process and the groupby column. Then, we group by the groupby
        # column and take a mean over the processes.
        #
        # Example:
        # If groupby column is "Name", this will return the average metric
        # value per process for each function name.
        return (
            self.events.loc[self.events["Event Type"] == "Enter"]
            .groupby([groupby_column, "Process"], observed=True)[metrics]
            .sum()
            .groupby(groupby_column)
            .mean()
        )
