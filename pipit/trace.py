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
            for id in set(enter_leave_df["TID"]):
                filtered_df = enter_leave_df.loc[enter_leave_df["TID"] == id]

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

    Returns:
    A dictionary of arrays containing nodes containing the function
    names, process id, duration in bin, and graph node.
    """

    def time_profile(self, start_time=1689900254, end_time=1794961473, time_interval=10000000):        
        if time_interval <= 0:
            raise Exception("Not Valid Time Interval")
        if start_time >= end_time or start_time < 0:
            raise Exception("Invalid start/end time")
        
        # Match the rows
        self.match_rows()
        
        # dict of bin times
        # {bin interval times: array of dictionarys}
        # Node contains name, pid, duration of how long in bin, graph node
        bins = np.arange(start_time, end_time, time_interval).tolist()
        
        bins = {key: [] for key in bins}
        
        keys_list = list(bins.keys())
        keys_list.append(end_time)
        
        # Create a sublist from trace events.
        sub = self.events.loc[(self.events['Event Type'] == 'Enter')]
        
        p = 'Process' if 'Process' in sub.columns else 'PID'
        t = 'Thread' if 'Thread' in sub.columns else 'TID'
        
        for index, row in sub.iterrows():    
            start_t = row['Timestamp (ns)']
            end_t = row['Matching Timestamp']
            
            # Case 1: Function extends whole time frame
            if start_t < start_time and end_t > end_time:
                # start bin
                bins[keys_list[0]].append(
                    {'Function Name': row['Name'], 'Time': keys_list[1] - start_time, p: row[p], t: row[t],'RangeID': row['RangeId']}
                )
                
                # middle bins
                for i in range(1, len(keys_list)-2):
                    bins[keys_list[i]].append(
                        {'Function Name': row['Name'], 'Time': time_interval, p: row[p], t: row[t],'RangeID': row['RangeId']}
                    )
                    
                # end bin
                bins[keys_list[len(keys_list)-2]].append( 
                    {'Function Name': row['Name'], 'Time': end_time - keys_list[len(keys_list) - 2], p: row[p], t: row[t],'RangeID': row['RangeId']}
                )
                
                
            # Case 2: Function starts before start_time and ends before end_time
            if start_t < start_time and end_t < end_time and end_t > start_time:
                # find end time bin
                end_bound = self.binary(
                    keys_list, 0, len(keys_list) - 1, end_t, False
                )
                
                # Add time to bin
                bins[keys_list[end_bound - 1]].append(
                    {'Function Name': row['Name'], 'Time': end_t - keys_list[end_bound - 1], p: row[p], t: row[t],'RangeID': row['RangeId']}
                )
                
                # Add time down the other bins if needed
                # -1 is need due to end range being exclusive
                for i in range(end_bound - 2, -1, -1):
                    bins[keys_list[i]].append(
                        {'Function Name': row['Name'], 'Time': time_interval, p: row[p], t: row[t],'RangeID': row['RangeId']}
                    )
            
            # Case 3: Function starts before end_time and continues past
            if start_t > start_time and end_t > end_time and start_t < end_time:
                
                # Find start and end bins for where the nodes need to be added
                start_bound = self.binary(
                    keys_list, 0, len(keys_list) - 1, start_t, True
                )
                
                # starting off the 
                bins[keys_list[start_bound]].append(
                    {'Function Name': row['Name'], 'Time': keys_list[start_bound + 1] - start_t, p: row[p], t: row[t],'RangeID': row['RangeId']}
                )
                
                # add to the rest of bins
                for i in range(start_bound + 1, len(keys_list) - 1):
                    if i == len(keys_list) - 2:
                        bins[keys_list[i]].append(
                            {'Function Name': row['Name'], 'Time': keys_list[len(keys_list) - 1] - keys_list[len(keys_list) - 2], p: row[p], t: row[t],'RangeID': row['RangeId']}
                        )
                    else:
                        bins[keys_list[i]].append(
                            {'Function Name': row['Name'], 'Time': time_interval, p: row[p], t: row[t],'RangeID': row['RangeId']}
                        )
            
            # Case 4: Function in between start and end time
            if start_t > start_time and end_t < end_time:
                print(row)
                
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
                        {'Function Name': row['Name'], 'Time': end_t - start_t, p: row[p], t: row[t],'RangeID': row['RangeId']}
                    )
                else:
                    bins[keys_list[start_bound]].append(
                        {'Function Name': row['Name'], 'Time': keys_list[start_bound + 1] - start_t, p: row[p], t: row[t],'RangeID': row['RangeId']}
                    )
                    bins[keys_list[end_bound - 1]].append(
                        {'Function Name': row['Name'], 'Time': end_t - keys_list[end_bound - 1], p: row[p], t: row[t], 'RangeID': row['RangeId']}
                    )
                    for i in range(start_bound + 1, end_bound - 1):
                        bins[keys_list[i]].append(
                            {'Function Name': row['Name'], 'Time': time_interval, p: row[p], t: row[t], 'RangeID': row['RangeId']}
                        )
    
        print(bins)
        print(keys_list)