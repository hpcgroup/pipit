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
        Some events are represented using two rows - an "Entry" and a "Exit"
        that correspond to each other. One event (two rows) correspond to a
        single function call.

        This function iterates through such events and does a few things:
        1. matches each entry and exit rows pair using numerical indices
        2. determines the children and parent of each event
        3. calculates the inclusive time for each function call
        4. determines the depth of each event in the call stack

        To reduce redundancy, the last three pieces of information listed above
        are stored only in the entry row of an event. The matching indices, by
        design, have to be stored in both so the user can find the corresponding
        "exit" row for an "entry" row of an event and vica-versa.
        """

        if "Inc Time (ns)" not in self.events.columns:
            # 6 new columns that will be added to the DataFrame
            parent = [float("nan") for i in range(len(self.events))]
            children = [None for i in range(len(self.events))]
            matching_time = [float("nan") for i in range(len(self.events))]
            matching_index = [float("nan") for i in range(len(self.events))]
            inc_time = [float("nan") for i in range(len(self.events))]
            depth = [float("nan") for i in range(len(self.events))]

            if "Thread ID" in self.events.columns:
                filter_set = set(self.events["Thread ID"])
                filter_col = "Thread ID"
            elif "Process ID" in self.events.columns:
                filter_set = set(self.events["Process ID"])
                filter_col = "Process ID"
            else:
                filter_col = None

            entry_exit_df = self.events.loc[
                self.events["Event Type"].isin(["Entry", "Exit"])
            ]

            # filter by thread/process and then iterate over events
            for id in filter_set:
                """
                filter the DataFrame by current ID so
                that the ordering of the rows make sense in the
                context of a call stack
                """
                if filter_col is not None:
                    filtered_df = entry_exit_df.loc[entry_exit_df[filter_col] == id]

                """
                Below are auxiliary lists used in the function.

                Since the DataFrame is filtered by Process/Thread ID,
                df_indices keeps trace of the DataFrame indices
                so that the metrics being calculated can be added to
                the correct row position in the DataFrame.

                The indices stack is used to keep track of the dataframe
                indices for the current callpate and calculate metrics &
                match parents with children accordingly.
                """
                curr_depth, stack, df_indices = 0, [], list(filtered_df.index)

                """
                copies of two columns as lists
                more efficient to iterate through these than the
                DataFrame itself (from what I've seen so far)
                """
                event_types, timestamps = list(filtered_df["Event Type"]), list(
                    filtered_df["Timestamp (ns)"]
                )

                # iterate through all events of the current ID
                for i in range(len(filtered_df)):
                    """
                    curr_df_index is the actual DataFrame index
                    that corresponds to the i'th row of filtered_df
                    """
                    curr_df_index = df_indices[i]

                    evt_type, timestamp = event_types[i], timestamps[i]

                    # if the row is the entry point of a function call
                    if evt_type == "Entry":
                        if curr_depth > 0:
                            """
                            if the current event is a child of another (curr depth > 0),
                            get the dataframe index of the parent event using the
                            stack and append the current DataFrame index to the
                            parent's children list
                            """

                            parent_df_index = stack[-1][0]

                            if children[parent_df_index] is None:
                                """
                                create a new list of children for the parent
                                if the current event is the first child
                                being added
                                """
                                children[parent_df_index] = [curr_df_index]
                            else:
                                children[parent_df_index].append(curr_df_index)

                            parent[curr_df_index] = parent_df_index

                        """
                        The inclusive time for a function is its Exit timestamp
                        subtracted by its Entry timestamp.
                        """
                        inc_time[curr_df_index] = -timestamp

                        """
                        Whenever an entry point for a function call is encountered,
                        add the DataFrame index and timestamp of the row to the stack
                        """
                        stack.append((curr_df_index, timestamp))

                        depth[curr_df_index] = curr_depth
                        curr_depth += 1  # increment the depth of the call stack

                    # if the row is the exit point of a function call
                    else:
                        """
                        get the DataFrame index and timestamp of the
                        corresponding entry row for the current exit
                        row by popping the stack
                        """
                        entry_df_index, entry_timestamp = stack.pop()

                        """
                        add the matching times to
                        the appropriate positions in the list
                        """
                        matching_time[entry_df_index] = timestamp
                        matching_time[curr_df_index] = entry_timestamp

                        """
                        add the matching DataFrame indices to
                        the appropriate positions in the list
                        """
                        matching_index[entry_df_index] = curr_df_index
                        matching_index[curr_df_index] = entry_df_index

                        """
                        by adding the exit timestamp, the
                        calculated time is Exit - Entry, which
                        is the inclusive time for the function call
                        """
                        inc_time[entry_df_index] += timestamp

                        curr_depth -= 1  # decrement the current depth of the call stack

            # needed because children is a list of nested lists and none objects
            children = np.array(children, dtype=object)

            """
            Create new columns of the DataFrame using
            the calculated metrics and lists above
            """
            self.events["Depth"] = depth
            self.events["Parent"] = parent
            self.events["Children"] = children
            self.events["Matching Index"] = matching_index
            self.events["Matching Timestamp"] = matching_time
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
            parents_df = self.events.loc[self.events["Children"].notnull()]

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

    def comm_matrix(self, comm_type="bytes"):
        """
        Communication Matrix for Peer-to-Peer (P2P) MPI messages

        Arguments:

        1) comm_type -
        string to choose whether the communication volume should be measured
        by bytes transferred between two processes or the number of messages
        sent (two choices - "bytes" or "counts")

        Returns:
        A 2D Numpy Array that represents the communication matrix for all P2P
        messages of the given trace

        Note:
        The first dimension of the returned 2d array
        is senders and the second dimension is receivers
        ex) comm_matrix[sender_rank][receiver_rank]
        """

        # get the list of ranks/process ids
        # (mpi messages are sent between processes)
        ranks = set(
            self.events.loc[self.events["Location Group Type"] == "PROCESS"][
                "Location Group ID"
            ]
        )

        # create a 2d numpy array that will be returned
        # at the end of the function
        communication_matrix = np.zeros(shape=(len(ranks), len(ranks)))

        # filter the dataframe by MPI Send and Isend events
        sender_dataframe = self.events.loc[
            self.events["Event Type"].isin(["MpiSend", "MpiIsend"]),
            ["Location Group ID", "Attributes"],
        ]

        # get the mpi ranks of all the sender processes
        sender_ranks = sender_dataframe["Location Group ID"].to_list()

        # get the corresponding mpi ranks of the receivers
        receiver_ranks = (
            sender_dataframe["Attributes"]
            .apply(lambda attrDict: attrDict["receiver"])
            .to_list()
        )

        # number of bytes communicated
        if comm_type == "bytes":
            # (1 communication is a single row in the sender dataframe)
            message_volumes = (
                sender_dataframe["Attributes"]
                .apply(lambda attrDict: attrDict["msg_length"])
                .to_list()
            )
        elif comm_type == "counts":
            # 1 message between the pairs of processes
            # for each row in the sender dataframe
            message_volumes = np.full(len(sender_dataframe), 1)

        for i in range(len(sender_ranks)):
            """
            loops through all the communication events and adds the
            message volumes to the corresponding entry of the 2d array
            using the sender and receiver ranks
            """
            communication_matrix[sender_ranks[i], receiver_ranks[i]] += message_volumes[
                i
            ]

        return communication_matrix
