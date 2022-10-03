# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np


class Node:
    def __init__(self, name, pid, dur, graph_node) -> None:
        self.name = name
        self.pid = pid
        self.dur = dur
        self.graph_node = graph_node

    def __str__(self) -> str:
        return (
            self.name
            + ": "
            + str(self.dur)
            + " "
            + str(self.pid)
            + " "
            + str(self.graph_node)
        )


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

    def binary(self, arr, low, high, x, start):
        mid = (high + low) // 2
        if high >= low:

            if arr[mid] == x:
                return mid

            elif arr[mid] > x:
                return self.binary(arr, low, mid - 1, x, start)

            else:
                return self.binary(arr, mid + 1, high, x, start)

        else:
            if start is True:
                return mid
            if start is False:
                return mid + 1

    # WIP

    """
    Time Profile for showing function times in time intervals

    Arguments:

    1) start_time (inclusive) -
    Input an int to choose where you want the trace to start

    2) end time (inclusive) -
    Input an int to choose where you want the trace to end

    3) time_interval -
    Input an int to choose how big you want the bins to be.
    Recommended you stay at 1000000ns
    Example Bin Size: 0-999999.9ns

    Returns:
    A dictionary of arrays containing nodes containing the function
    names, process id, duration in bin, and graph node.

    Issues:
    1) Need to optimize if possible.
    """

    def time_profile(self, start_time=0, end_time=245412000, time_interval=100000000):
        if time_interval <= 0:
            raise Exception("Not Valid Time Interval")
        if start_time >= end_time or start_time < 0:
            raise Exception("Invalid start/end time")

        # dict of bin times
        # {bin interval times: array of nodes}
        # Node contains name, pid, duration of how long in bin, graph node
        bins = np.arange(start_time, end_time, time_interval).tolist()

        bins = {key: [] for key in bins}

        # Add cushion for calculating duration time
        bins[end_time] = []

        # Create a list of keys to use in binary search function
        keys_list = list(bins.keys())

        # Create a subset of events between the specific time frame
        sub_main = self.events.loc[
            (self.events["Time"] >= start_time) & (self.events["Time"] < end_time)
        ]

        # Create a list of Different processes for filtering
        process = sub_main["Process"].to_numpy()
        process = np.unique(process).tolist()

        for j in process:
            # Create another subset of the specific process
            sub = sub_main.loc[sub_main["Process"].isin([j])]
            sub.reset_index(drop=True, inplace=True)
            # dict of dicts
            # {Function Name: {Graph_Node: Start Time}}
            # Reason is that if a function has multiple calls
            # with many different graph nodes, allows access in less time
            stack = dict()

            # Going through the subset of certain process
            for i in range(len(sub)):
                data = sub.iloc[i]
                if data["Event Type"] == "Enter":
                    if data["Function Name"] not in stack:
                        stack[data["Function Name"]] = {
                            str(data["Graph_Node"]): data["Time"]
                        }
                    else:
                        stack[data["Function Name"]][str(data["Graph_Node"])] = data[
                            "Time"
                        ]

                else:
                    # See if time profile started in the middle of a function
                    if (
                        data["Function Name"] not in stack
                        or str(data["Graph_Node"]) not in stack[data["Function Name"]]
                    ):
                        end_t = data["Time"]

                        # Find end time bin
                        end_bound = self.binary(
                            keys_list, 0, len(keys_list) - 1, end_t, False
                        )

                        # Add time to bin
                        bins[keys_list[end_bound - 1]].append(
                            Node(
                                data["Function Name"],
                                data["Process"],
                                float(end_t - keys_list[end_bound - 1]),
                                data["Graph_Node"],
                            )
                        )

                        # Add time down the other bins if needed
                        for i in range(end_bound - 1, 0, -1):
                            bins[keys_list[i]].append(
                                Node(
                                    data["Function Name"],
                                    data["Process"],
                                    time_interval - 0.1,
                                    data["Graph_Node"],
                                )
                            )

                    else:
                        start_t = stack[data["Function Name"]][str(data["Graph_Node"])]
                        end_t = data["Time"]

                        # remove value from dict
                        stack[data["Function Name"]].pop(str(data["Graph_Node"]))

                        # Find start and end bins for where the nodes need to be added
                        start_bound = self.binary(
                            keys_list, 0, len(keys_list) - 1, start_t, True
                        )

                        end_bound = self.binary(
                            keys_list, 0, len(keys_list) - 1, end_t, False
                        )

                        # If the function time fits in one bin
                        if start_bound == end_bound - 1:
                            bins[keys_list[start_bound]].append(
                                Node(
                                    data["Function Name"],
                                    data["Process"],
                                    float(end_t - start_t),
                                    data["Graph_Node"],
                                )
                            )
                        # if the function time spans across
                        # multiple bins with exits given
                        else:
                            bins[keys_list[start_bound]].append(
                                Node(
                                    data["Function Name"],
                                    data["Process"],
                                    float(keys_list[start_bound + 1] - start_t) - 0.1,
                                    data["Graph_Node"],
                                )
                            )
                            bins[keys_list[end_bound - 1]].append(
                                Node(
                                    data["Function Name"],
                                    data["Process"],
                                    float(end_t - keys_list[end_bound - 1]),
                                    data["Graph_Node"],
                                )
                            )
                            for i in range(start_bound + 1, end_bound - 1):
                                bins[keys_list[i]].append(
                                    Node(
                                        data["Function Name"],
                                        data["Process"],
                                        time_interval - 0.1,
                                        data["Graph_Node"],
                                    )
                                )
            # if the stack has left over functions without any exits
            if len(stack) > 0:
                for k in stack:
                    # check if function has events
                    if len(stack[k]) > 0:
                        inner_set = stack[k]
                        for m in inner_set:
                            # Find start time, and starting bin
                            start_t = inner_set[m]
                            start_bound = self.binary(
                                keys_list, 0, len(keys_list) - 1, start_t, True
                            )
                            # add node to starting bin
                            bins[keys_list[start_bound]].append(
                                Node(
                                    k,
                                    j,
                                    float(keys_list[start_bound + 1] - start_t) - 0.1,
                                    m,
                                )
                            )
                            # add nodes to the rest of the bins
                            for i in range(start_bound + 1, len(keys_list)):
                                bins[keys_list[i]].append(
                                    Node(k, j, time_interval - 0.1, m)
                                )

        # remove buffer
        bins.pop(end_time)
        return bins
