# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pipit.trace


class NsightReader:
    """Reader for Nsight trace files"""

    def __init__(self, file_name) -> None:
        self.file_name = file_name
        self.df = None

    """
    This read function directly takes in a csv of the trace report and
    utilizes pandas to convert it from a csv into a dataframe.
    """

    def read(self):
        # Read in csv
        self.df = pd.read_csv(self.file_name)

        # Grab the set of the column PID columns to see if mutliprocess
        pid = set(self.df["PID"])

        # check if PID and TID are NOT the same. singlethreaded or multithreaded
        if self.df["PID"].equals(self.df["TID"]) is False:
            # get the list of pid
            pid_list = list(pid)

            # group the pids together and give each process it's own set of threads
            # Example
            #       Process (8226) 0  Process (8227) 1
            #       Thread (8226) 0   Thread (8227) 0
            #       Thread (8227) 1   Thread (8228) 1
            for i in pid_list:
                # grabbing the TIDs from distinct PID
                tid = self.df.loc[self.df.PID == i, "TID"]

                # Creating a dictionary and then incrementing each value
                tid_dict = dict.fromkeys(tid, 0)
                tid_dict.update((k, i) for i, k in enumerate(tid_dict))
                # Setting the Thread ID using the dictionary and the TID
                self.df.loc[self.df["PID"] == i, "Thread ID"] = self.df["TID"].map(
                    tid_dict
                )
            # Converting Thread ID from float to int
            self.df["Thread ID"] = self.df["Thread ID"].astype(int)

        # check if PID set is > 1, if so multiprocess or single process
        if len(pid) > 1:
            pid_dict = dict.fromkeys(pid, 0)
            pid_dict.update((k, i) for i, k in enumerate(pid_dict))
            self.df["Process ID"] = self.df["PID"]
            self.df["Process ID"].replace(pid_dict, inplace=True)

        # Copy self.df to create enter and exit rows
        df2 = self.df.copy()

        # Create new columns for self.df with start time to create entry rows
        self.df["Event Type"] = "Entry"
        self.df["Timestamp (ns)"] = self.df["Start (ns)"]

        # Create new columns for df2 with end time to create exit rows
        df2["Event Type"] = "Exit"
        df2["Timestamp (ns)"] = df2["End (ns)"]

        # Combine dataframes together
        self.df = pd.concat([self.df, df2])

        # Tidy Dataframe
        self.df.drop(["Start (ns)", "End (ns)"], axis=1, inplace=True)

        self.df.sort_values(by="Timestamp (ns)", ascending=True, inplace=True)

        self.df.reset_index(drop=True, inplace=True)

        self.df = self.df.astype(
            {
                "Event Type": "category",
                "Name": "category",
                "PID": "category",
                "TID": "category",
            }
        )

        # Grabbing the list of columns and rearranging them to put
        # Timestamp, Event Types, Name, Thread ID (potentially),
        # Process ID(potentially) in the front of the dataframe
        cols = list(self.df)
        cols.insert(0, cols.pop(cols.index("Timestamp (ns)")))
        cols.insert(1, cols.pop(cols.index("Event Type")))
        cols.insert(2, cols.pop(cols.index("Name")))

        if "Process ID" in self.df.columns:
            cols.insert(3, cols.pop(cols.index("Process ID")))
            if "Thread ID" in self.df.columns:
                cols.insert(3, cols.pop(cols.index("Thread ID")))

        elif "Thread ID" in self.df.columns:
            cols.insert(3, cols.pop(cols.index("Thread ID")))

        # Applying the column list to the dataframe to rearrange
        self.df = self.df.loc[:, cols]

        return pipit.trace.Trace(None, self.df)
