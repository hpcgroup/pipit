# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd


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

    def __event_locations(self):
        """
        private helper function to return a set of the thread or process ids
        (most granular location that event happened on)
        """

        if "Thread ID" in self.events.columns:
            return (set(self.events["Thread ID"]), "Thread ID")
        elif "Process ID" in self.events.columns:
            return (set(self.events["Process ID"]), "Process ID")
        else:
            return (set(), None)

    def select(
        self,
        query=None,
        events=None,
        rank=None,
        max_num_ranks=None,
        min_time=None,
        max_time=None,
        min_matching_time=None,
        max_matching_time=None,
        event_type=None,
        name=None,
        group=None,
        max_num_events=None,
    ) -> pd.DataFrame:
        """
        Return events dataframe filtered with provided params

        Args:
            events: Events DataFrame to apply filters to
            query: Pandas query to apply
            rank: Rank, list of ranks, or dict with ranks to include/exclude
            max_num_ranks: Maximum number of ranks
            min_time: Starting timestamp
            max_time: Ending timestamp
            min_matching_time: Starting matching time
            max_matching_time: Ending matching time
            event_type: Event type, list, or dict
            name: Name, list of names, or dict with names to include/exclude
            group: Function group name, list, or dict
            max_num_events: Maximum number of events

        Returns: pd.DataFrame
        """
        if events is None:
            events = self.events

        df = events.copy(deep=False)

        # Filter by Pandas query
        # See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html
        if query is not None:
            df = df.query(query)

        # Filter by rank
        if rank is not None:
            if isinstance(rank, int):
                rank = [rank]

            if type(rank) is list:
                rank = {"include": rank}

            if "include" in rank:
                df = df[df["Process ID"].isin(rank["include"])]

            if "exclude" in rank:
                df = df[-df["Process ID"].isin(rank["exclude"])]

        # Filter by max_num_ranks
        if max_num_ranks is not None:
            ranks = df["Process ID"].unique()
            num_ranks = len(ranks)

            if max_num_ranks < num_ranks:
                # Select num_ranks ranks evenly from all the ranks
                allowed_indices = np.round(
                    np.linspace(0, num_ranks - 1, max_num_ranks)
                ).astype(int)
                allowed_ranks = ranks[allowed_indices]
                df = df[df["Process ID"].isin(allowed_ranks)]

        # Filter by min_time and max_time
        if min_time is not None:
            df = df[df["Timestamp (ns)"] > min_time]

        if max_time is not None:
            df = df[df["Timestamp (ns)"] < max_time]

        # Filter by min_matching_time and max_matching_time
        if min_matching_time is not None:
            df = df[df["Matching Timestamp"] > min_matching_time]

        if max_matching_time is not None:
            df = df[df["Matching Timestamp"] < max_matching_time]

        # Filter by event_type
        if event_type is not None:
            if isinstance(event_type, str):
                event_type = [event_type]

            if type(event_type) is list:
                event_type = {"include": event_type}

            if "include" in event_type:
                df = df[df["Event Type"].isin(event_type["include"])]

            if "exclude" in event_type:
                df = df[-df["Event Type"].isin(event_type["exclude"])]

        # Filter by name
        if name is not None:
            if isinstance(name, str):
                name = [name]

            if type(name) is list:
                name = {"include": name}

            if "include" in name:
                df = df[df["Name"].isin(name["include"])]

            if "exclude" in name:
                df = df[-df["Name"].isin(name["exclude"])]

        # Filter by function group name
        if group is not None:
            if isinstance(group, str):
                group = [group]

            if type(group) is list:
                group = {"include": group}

            if "include" in group:
                df = df[df["Name"].startswith(tuple(name["include"]))]

            if "exclude" in group:
                df = df[-df["Name"].startswith(tuple(name["exclude"]))]

        # Sample by max_num_events
        if max_num_events is not None and len(df) > max_num_events:
            df = df.sample(n=max_num_events)

        return df

    def match_rows(self):
        if "Matching Index" not in self.events.columns:
            """
            Two columns to be added to dataframe:
            "Matching Index" and "Matching Timestamp"

            Matches dataframe indices and timestamps
            between corresponding entry and exit rows.
            """
            matching_indices = [float("nan")] * len(self.events)
            matching_times = [float("nan")] * len(self.events)

            filter_set, filter_col = self.__event_locations()

            entry_exit_df = self.events.loc[
                self.events["Event Type"].isin(["Entry", "Exit"])
            ]

            # Filter by Thread/Process ID
            for id in filter_set:
                if filter_col is not None:
                    filtered_df = entry_exit_df.loc[entry_exit_df[filter_col] == id]

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

                    if evt_type == "Entry":
                        # Add current dataframe index and timestamp to stack
                        stack.append((curr_df_index, curr_timestamp))
                    else:
                        # Pop corresponding entry event's dataframe index and timestamp
                        entry_df_index, entry_timestamp = stack.pop()

                        # Fill in the lists with the matching values
                        matching_indices[entry_df_index] = curr_df_index
                        matching_indices[curr_df_index] = entry_df_index

                        matching_times[entry_df_index] = curr_timestamp
                        matching_times[curr_df_index] = entry_timestamp

            self.events["Matching Index"] = matching_indices
            self.events["Matching Timestamp"] = matching_times

    def calc_inc_time(self):
        # Adds "Inc Time" column
        if "Inc Time" not in self.events.columns:
            if "Matching Timestamp" not in self.events.columns:
                self.match_rows()

            # Uses matching timestamp to calculate the inclusive time
            self.events["Inc Time"] = (
                self.events["Matching Timestamp"] - self.events["Timestamp (ns)"]
            ).abs()

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

            filter_set, filter_col = self.__event_locations()

            entry_exit_df = self.events.loc[
                self.events["Event Type"].isin(["Entry", "Exit"])
            ]

            for id in filter_set:
                if filter_col is not None:
                    filtered_df = entry_exit_df.loc[entry_exit_df[filter_col] == id]

                curr_depth, stack = 0, []
                df_indices, event_types = list(filtered_df.index), list(
                    filtered_df["Event Type"]
                )

                for i in range(len(filtered_df)):
                    curr_df_index, evt_type = df_indices[i], event_types[i]

                    if evt_type == "Entry":
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

                        # add entry dataframe index to stack
                        stack.append(curr_df_index)
                    else:
                        entry_df_index = stack.pop()

                        """
                        storing depth and parent in both entry and exit rows
                        since they are floats.

                        children stored as nan in exit row and can be found
                        using matching index for avoiding redundant memory.
                        """
                        depth[curr_df_index] = depth[entry_df_index]
                        parent[curr_df_index] = parent[entry_df_index]

                        curr_depth -= 1

            self.events["Depth"] = depth
            self.events = self.events.astype({"Depth": "category"})

            # parent categorical?
            self.events["Parent"], self.events["Children"] = parent, children

    def calc_exc_time(self):
        if "Exc Time" not in self.events.columns:
            if "Inc Time" not in self.events.columns:
                self.calc_inc_time()
            if "Children" not in self.events.columns:
                self.calling_relationships()

            # start out with exc times being a copy of inc times
            exc_times = list(self.events["Inc Time"])
            inc_times = list(self.events["Inc Time"])

            # Filter to events that have children
            filtered_df = self.events.loc[self.events["Children"].notnull()]
            parent_df_indices, children = list(filtered_df.index), list(
                filtered_df["Children"]
            )

            # Iterate through the events that are parents
            for i in range(len(filtered_df)):
                curr_parent_idx, curr_children = parent_df_indices[i], children[i]
                for child_idx in curr_children:
                    # Subtract child's inclusive time to update parent's exclusive time
                    exc_times[curr_parent_idx] -= inc_times[child_idx]

            # Set exit rows exc times to matching entry rows
            matching_indices = list(filtered_df["Matching Index"])
            for i in range(len(filtered_df)):
                exc_times[int(matching_indices[i])] = exc_times[parent_df_indices[i]]

            self.events["Exc Time"] = exc_times

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
        ranks = self.events["Process ID"].unique()

        # create a 2d numpy array that will be returned
        # at the end of the function
        communication_matrix = np.zeros(shape=(len(ranks), len(ranks)))

        # filter the dataframe by MPI Send and Isend events
        sender_dataframe = self.events.loc[
            self.events["Event Type"].isin(["MpiSend", "MpiIsend"]),
            ["Process ID", "Attributes"],
        ]

        # get the mpi ranks of all the sender processes
        sender_ranks = sender_dataframe["Process ID"].to_list()

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
