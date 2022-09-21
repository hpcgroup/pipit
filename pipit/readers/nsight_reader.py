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

        graph = self.create_cct()

        print(graph.get_graphs()[0])
        print(graph.calling_context_id_map)

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
            print(node)
            df.at[i, "Graph_Node"] = node
            df.at[i, "Level"] = node.level

        # Create new columns for df2 with end time
        df2["Event Type"] = "Exit"
        df2["Time"] = df2["End (ns)"]

        for i in range(len(df2)):
            node = graph.get_node(df2.iloc[i]["RangeStack"])
            df2.at[i, "Graph_Node"] = node
            df2.at[i, "Level"] = node.level
            
        df2.Level = df2.Level.astype(int)

        # Combine dataframes together
        df = pd.concat([df, df2])

        # Tidy Dataframe
        df.drop(["Start (ns)", "End (ns)"], axis=1, inplace=True)

        df.sort_values(by="Time", ascending=True, inplace=True)

        df.reset_index(drop=True, inplace=True)

        return pipit.trace.Trace(None, df)

    
    """
    This create_cct function takes in a dataframe and shows the user
    how the cct would look like within their program.

    TODOS:
    Fix to work with multiple roots
    Add more Comments

    """

    def create_cct(self):
        df = self.df

        # Creating calling context graph
        call_graph = graph.Graph()
        callpath = dict()
        func_path = []
        stack = []

        for i in range(len(df)):
            # create a root

            # check if true
            if call_graph.is_empty():
                root = graph.Node(-1, df.iloc[i]["Name"], None)
                root.add_calling_context_id(df.iloc[i]["RangeStack"])
                root.add_time(df.iloc[i]["Start (ns)"], df.iloc[i]["End (ns)"])

                # add root to tree
                call_graph.add_root(root)
                call_graph.add_to_map(root.calling_context_ids[0], root)

                # add root stack
                stack.append(df.iloc[i])
                func_path.append(df.iloc[i]["Name"])

                path = "->".join(func_path)

                callpath[path] = root

            else:
                curr_start = df.iloc[i]["Start (ns)"]
                prev_end = stack[len(stack) - 1]["End (ns)"]

                # CASE 1 Function in another function
                if prev_end > curr_start:
                                        
                    path = "->".join(func_path)
                    func_path.append(df.iloc[i]["Name"])
                    path2 = "->".join(func_path)

                    if path2 not in callpath:
                        parent = callpath[path]
                        node = graph.Node(-1, df.iloc[i]["Name"], parent)
                        node.add_calling_context_id(df.iloc[i]["RangeStack"])
                        node.add_time(df.iloc[i]["Start (ns)"], df.iloc[i]["End (ns)"])
                        parent.add_child(node)

                        callpath[path2] = node
                        call_graph.add_to_map(df.iloc[i]["RangeStack"], node)
                        
                    else:
                        callpath[path2].add_calling_context_id(df.iloc[i]["RangeStack"])
                        call_graph.add_to_map(df.iloc[i]["RangeStack"], callpath[path2])


                    # Adding child function to stack
                    stack.append(df.iloc[i])


                # Case #2 Function is outside of the previous function
                else:
                    # Fix issue with multiple roots
                    while stack[len(stack) - 1]["End (ns)"] < curr_start:
                        stack.pop()
                        func_path.pop()

                    path = "->".join(func_path)
                    func_path.append(df.iloc[i]["Name"])
                    path2 = "->".join(func_path)

                    if path2 not in callpath:
                        parent = callpath[path]
                        node = graph.Node(-1, df.iloc[i]["Name"], parent)
                        node.add_calling_context_id(df.iloc[i]["RangeStack"])
                        node.add_time(df.iloc[i]["Start (ns)"], df.iloc[i]["End (ns)"])
                        parent.add_child(node)

                        callpath[path2] = node
                        call_graph.add_to_map(df.iloc[i]["RangeStack"], node)

                    else:
                        callpath[path2].add_calling_context_id(df.iloc[i]["RangeStack"])
                        call_graph.add_to_map(df.iloc[i]["RangeStack"], callpath[path2])

                    # Adding child function to stack
                    stack.append(df.iloc[i])

        return call_graph