# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pipit.trace


class NSightReader:
    """Reader for NSight trace files"""

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

        # Copy data into new dataframe
        df = self.df

        pid, tid = set(df["PID"]), set(df["TID"])

        df = df.astype(
            {
                "PID": "category",
                "TID": "category",
            }
        )

        # check if multi-process, single-threaded trace, if so remove tid column
        if len(pid) > 1:
            if len(pid) == len(tid):
                df.drop(columns="TID", inplace=True)
        else:
            # remove pid column, single process thread
            df.drop(columns="PID", inplace=True)
            # remove tid column for  single-threaded trace and single-process
            if len(tid) == 1:
                df.drop(columns="TID", inplace=True)

        if "PID" in df.columns:
            pid_dict = dict.fromkeys(pid, 0)
            pid_dict.update((k, i) for i, k in enumerate(pid_dict))
            df["PID"].replace(pid_dict, inplace=True)
            df.rename(columns={"PID": "Parent ID"}, inplace=True)

        if "TID" in df.columns:
            tid_dict = dict.fromkeys(tid, 0)
            tid_dict.update((k, i) for i, k in enumerate(tid_dict))
            df["TID"].replace(tid_dict, inplace=True)
            df.rename(columns={"TID": "Thread ID"}, inplace=True)

        df2 = df.copy()

        # Create new columns for df with start time
        df["Event Type"] = "Entry"
        df["Timestamp (ns)"] = df["Start (ns)"]

        # Create new columns for df2 with end time
        df2["Event Type"] = "Exit"
        df2["Timestamp (ns)"] = df2["End (ns)"]

        # Combine dataframes together
        df = pd.concat([df, df2])

        # Tidy Dataframe
        df.drop(["Start (ns)", "End (ns)"], axis=1, inplace=True)

        df.sort_values(by="Timestamp (ns)", ascending=True, inplace=True)

        df.reset_index(drop=True, inplace=True)

        df = df.astype(
            {
                "Event Type": "category",
                "Name": "category",
            }
        )

        print(df.dtypes)

        return pipit.trace.Trace(None, df)
