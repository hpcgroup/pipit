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

    @staticmethod
    def from_nsight(filename):
        """Read an Nsight trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.nsight_reader import NsightReader

        return NsightReader(filename).read()

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

    def match_rows(self):
        if "Matching Index" not in self.events.columns:
            """
            Two columns to be added to dataframe:
            "Matching Index" and "Matching Timestamp"

            Matches dataframe indices and timestamps
            between corresponding enter and leave rows.
            """
            matching_indices = [float("nan")] * len(self.events)
            matching_times = [float("nan")] * len(self.events)

            enter_leave_df = self.events.loc[
                self.events["Event Type"].isin(["Enter", "Leave"])
            ]

            # Filter by Thread/Process
            for id in set(enter_leave_df["Thread"]):
                filtered_df = enter_leave_df.loc[enter_leave_df["Thread"] == id]

                stack = []
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
                        # Pop corresponding enter event's dataframe index and timestamp
                        enter_df_index, enter_timestamp = stack.pop()

                        # Fill in the lists with the matching values
                        matching_indices[enter_df_index] = curr_df_index
                        matching_indices[curr_df_index] = enter_df_index

                        matching_times[enter_df_index] = curr_timestamp
                        matching_times[curr_df_index] = enter_timestamp

            self.events["Matching Index"] = matching_indices
            self.events["Matching Timestamp"] = matching_times

    def calling_relationships(self):
        """
        Three columns to be added to dataframe:
        "Depth", "Parent", and "Children"

        Depth is level in the call tree starting from 0.
        Parent is the dataframe index of a row's parent event.
        Children is a list of dataframe indices of a row's children events.
        """

        if "Children" not in self.events.columns:
            children = [None] * len(self.events)
            depth, parent = [float("nan")] * len(self.events), [float("nan")] * len(
                self.events
            )

            enter_leave_df = self.events.loc[
                self.events["Event Type"].isin(["Enter", "Leave"])
            ]

            for id in set(enter_leave_df["Thread"]):
                filtered_df = enter_leave_df.loc[enter_leave_df["Thread"] == id]

                curr_depth, stack = 0, []
                df_indices, event_types = list(filtered_df.index), list(
                    filtered_df["Event Type"]
                )

                for i in range(len(filtered_df)):
                    curr_df_index, evt_type = df_indices[i], event_types[i]

                    if evt_type == "Enter":
                        if curr_depth > 0:  # if event is a child of some other event
                            parent_df_index = stack[-1]

                            if children[parent_df_index] is None:
                                """
                                create a new list of children for the parent if
                                the current event is the first child being added
                                """
                                children[parent_df_index] = [curr_df_index]
                            else:
                                children[parent_df_index].append(curr_df_index)

                            parent[curr_df_index] = parent_df_index

                        depth[curr_df_index] = curr_depth
                        curr_depth += 1

                        # add enter dataframe index to stack
                        stack.append(curr_df_index)
                    else:
                        enter_df_index = stack.pop()

                        """
                        storing depth and parent in both enter and leave rows
                        since they are floats.

                        children stored as nan in leave row and can be found
                        using matching index for avoiding redundant memory.
                        """
                        depth[curr_df_index] = depth[enter_df_index]
                        parent[curr_df_index] = parent[enter_df_index]

                        curr_depth -= 1

            self.events["Depth"], self.events["Parent"], self.events["Children"] = (
                depth,
                parent,
                children,
            )

            self.events = self.events.astype(
                {"Depth": "category", "Parent": "category"}
            )
