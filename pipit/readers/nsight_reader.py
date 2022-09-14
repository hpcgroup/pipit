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

        graph = self.call_graph()

        # Copy data into new dataframe
        df = self.df
        df2 = df.copy()

        # Create new columns for df with start time
        df["Event Type"] = "Enter"
        df["Time"] = df["Start (ns)"]
        df["Graph_Node"] = None
        df["Level"] = None

        for i in range(len(df)):
            node = graph.get_node(df.iloc[i]["RangeStack"])
            df.at[i, "Graph_Node"] = node
            df.at[i, "Level"] = node.get_level()

        # Create new columns for df2 with end time
        df2["Event Type"] = "Exit"
        df2["Time"] = df2["End (ns)"]

        for i in range(len(df2)):
            node = graph.get_node(df2.iloc[i]["RangeStack"])
            df2.at[i, "Graph_Node"] = node
            df2.at[i, "Level"] = node.get_level()

        df2.Level = df2.Level.astype(int)

        # Combine dataframes together
        df = pd.concat([df, df2])

        # Tidy Dataframe
        df.drop(["Start (ns)", "End (ns)"], axis=1, inplace=True)

        df.sort_values(by="Time", ascending=True, inplace=True)

        df.reset_index(drop=True, inplace=True)

        return pipit.trace.Trace(None, df)

    """
    This call graph function takes in a dataframe and shows the user
    how the call graph would look like within their program.
    """

    def call_graph(self):
        df = self.df

        # Creating call graph
        call_graph = graph.Graph()

        # Creating a stack
        stack = []

        for i in range(len(df)):
            # create a root
            if call_graph.is_empty():
                root = graph.Node(-1, df.iloc[i]["Name"], None)
                root.add_calling_context_id(df.iloc[i]["RangeStack"])
                root.add_time(df.iloc[i]["Start (ns)"], df.iloc[i]["End (ns)"])

                # add root to tree
                call_graph.add_root(root)
                call_graph.add_to_map(root.calling_context_ids[0], root)

                # add root stack
                stack.append(df.iloc[i])

            # If existing root
            else:
                curr_start = df.iloc[i]["Start (ns)"]
                prev_end = stack[len(stack) - 1]["End (ns)"]

                # CASE 1 Function in another function
                if prev_end > curr_start:
                    parent = call_graph.get_node(stack[len(stack) - 1]["RangeStack"])
                    node = graph.Node(-1, df.iloc[i]["Name"], parent)
                    node.add_calling_context_id(df.iloc[i]["RangeStack"])
                    node.add_time(df.iloc[i]["Start (ns)"], df.iloc[i]["End (ns)"])
                    parent.add_child(node)

                    call_graph.add_to_map(node.calling_context_ids[0], node)

                    # Adding child function to stack
                    stack.append(df.iloc[i])

                # Case #2 Function is outside of the previous function
                else:

                    # Fix issue with multiple roots
                    while stack[len(stack) - 1]["End (ns)"] < curr_start:
                        stack.pop()

                    parent = call_graph.get_node(stack[len(stack) - 1]["RangeStack"])
                    node = graph.Node(-1, df.iloc[i]["Name"], parent)
                    node.add_calling_context_id(df.iloc[i]["RangeStack"])
                    node.add_time(df.iloc[i]["Start (ns)"], df.iloc[i]["End (ns)"])
                    parent.add_child(node)

                    call_graph.add_to_map(node.calling_context_ids[0], node)

                    # Adding child function to stack
                    stack.append(df.iloc[i])

        return call_graph
