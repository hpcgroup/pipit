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

        # Grab the sorted set of the column PID columns to see if
        # mutliprocess and convert to a list
        pid = list(set(self.df["PID"]))

        # check if PID and TID are NOT the same. singlethreaded or multithreaded
        if self.df["PID"].equals(self.df["TID"]) is False:

            # Group the pids together and give each process it's own set of threads
            for i in pid:
                # Seeing where the PIDs match
                mask = self.df["PID"] == i
                # Creating a set from the matching PID rows dataframe of the TIDs
                tid = set(self.df[mask]["TID"])
                # Getting the TID set, creating a dictionary,
                # and increment the values (0,1,2,...)
                tid_dict = dict(zip(tid, range(0, len(tid))))
                # Setting the Thread column using the dictionary, mask and the TID
                self.df.loc[mask, "Thread"] = self.df["TID"].map(tid_dict)

            # Converting Thread from float to int
            self.df["Thread"] = self.df["Thread"].astype(int)

        # check if PID set is > 1, if so multiprocess or single process
        if len(pid) > 1:
            # Set Process column to PID
            self.df["Process"] = self.df["PID"]
            # Getting the PID set, creating a dictionary,
            # and increment the values (0,1,2,...)
            pid_dict = dict(zip(pid, range(0, len(pid))))
            # Using the dictionary to replace the Process values
            self.df["Process"].replace(pid_dict, inplace=True)

        # Copy self.df to create enter and leave rows
        df2 = self.df.copy()

        # Create new columns for self.df with start time to create enter rows
        self.df["Event Type"] = "Enter"
        self.df["Timestamp (ns)"] = self.df["Start (ns)"]

        # Create new columns for df2 with end time to create leave rows
        df2["Event Type"] = "Leave"
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
        # Timestamp, Event Types, Name, Thread (potentially),
        # Process(potentially) in the front of the dataframe
        cols = list(self.df)
        cols.insert(0, cols.pop(cols.index("Timestamp (ns)")))
        cols.insert(1, cols.pop(cols.index("Event Type")))
        cols.insert(2, cols.pop(cols.index("Name")))

        if "Process" in self.df.columns:
            cols.insert(3, cols.pop(cols.index("Process")))
            if "Thread" in self.df.columns:
                cols.insert(3, cols.pop(cols.index("Thread")))

        elif "Thread" in self.df.columns:
            cols.insert(3, cols.pop(cols.index("Thread")))

        # Applying the column list to the dataframe to rearrange
        self.df = self.df.loc[:, cols]

        return pipit.trace.Trace(None, self.df)
