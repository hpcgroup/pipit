# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pipit.trace
import pipit.graph as graph


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

        # Checks if multi-process trace
        if len(set(df.loc[:, "PID"])) == 1:
            df.drop(["PID"], axis=1, inplace=True)
            # Checks if multi-threaded trace 
            if len(set(df.loc[:, "TID"])) == 1:
                df.drop(["TID"], axis=1, inplace=True)

        # If multi-processed, checks if also multi-threaded
        else:
            if set(df.loc[:, "PID"]) == set(df.loc[:, "TID"]):
                df.drop(["TID"], axis=1, inplace=True)


        df.sort_values(by="Timestamp (ns)", ascending=True, inplace=True)

        df.reset_index(drop=True, inplace=True)

        return pipit.trace.Trace(None, df)
