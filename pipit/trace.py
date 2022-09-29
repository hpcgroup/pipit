# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import matplotlib.pyplot as plt
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

    def time_profile(self, time_bins=100):
        if time_bins <= 0:
            return "Not Valid Amount of Bins"

        # Start time and End time from trace file
        start_time = self.events["Time"].min()
        end_time = self.events["Time"].max()

        bin_size = (end_time - start_time) / time_bins

        intervals = [start_time + bin_size * interval for interval in range(time_bins)]

        tp = {}

        for i in range(0, len(self.events.index), 2):
            # Get Function Name
            func = self.events.iloc[i]["Name"]

            tp[func] = [float(0)] * (len(intervals))

        for i in range(0, len(self.events.index), 2):
            # Find Start and End time for every function
            start = self.events.iloc[i]["Time"]
            end = self.events.iloc[i + 1]["Time"]
            func = self.events.iloc[i]["Name"]

            # Binary Search for index starting and index ending
            start_bound = self.binary(intervals, 0, len(intervals) - 1, start, True)
            end_bound = self.binary(intervals, 0, len(intervals) - 1, end, False)

            # Time within
            if start_bound == end_bound - 1:
                tp[func][start_bound] += float(end - start)

            else:
                tp[func][start_bound] += intervals[start_bound + 1] - start
                tp[func][start_bound + 1 : end_bound - 1] += bin_size
                tp[func][end_bound - 1] += end - intervals[end_bound - 1]

        tp_df = pd.DataFrame(tp)

        tp_df["Bins"] = intervals

        print(tp_df)

        tp_df.plot.bar(x="Bins", stacked=True, title="Time in Functions")

        plt.show()

        return
