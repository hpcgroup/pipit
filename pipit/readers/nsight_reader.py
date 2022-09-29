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

        # Copy data into new dataframe
        df = self.df
        df2 = df.copy()

        # Create new columns for df with start time
        df["Event Type"] = "Enter"
        df["Time"] = df["Start (ns)"]

        # # Update this to work with MAD data
        # for i in range(len(df)):
        #     node = graph.get_node(df.iloc[i]["RangeStack"])
        #     df.at[i, "Graph_Node"] = node
        #     df.at[i, "Level"] = node.level

        df["Graph_Node"] = df.apply(lambda x: graph.get_node(x["RangeStack"]), axis=1)
        df["Level"] = df.apply(lambda x: graph.get_node(x["RangeStack"]).level, axis=1)

        # Create new columns for df2 with end time
        df2["Event Type"] = "Exit"
        df2["Time"] = df2["End (ns)"]

        df2["Graph_Node"] = df2.apply(lambda x: graph.get_node(x["RangeStack"]), axis=1)
        df2["Level"] = df2.apply(lambda x: graph.get_node(x["RangeStack"]).level, axis=1)

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
    Fixed taking in mutliple roots, but context IDS will be weird
    if range stack is the same for multiple roots
    Add more Comments

    """

    def create_cct(self):
        df = self.df

        # Creating calling context graph
        call_graph = graph.Graph()
        callpath = dict()
        stack, func_path = [], []
        prev_rs_len, rs_len = 0, 0

        for i in range(len(df)):
            rs_len = len(df.iloc[i]["RangeStack"].split(":")) - 1

            # If root
            if rs_len == 1:
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

            # If child
            elif rs_len > prev_rs_len:
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

            else:
                for j in range(0, (prev_rs_len + 1) - rs_len):
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

            prev_rs_len = rs_len

        return call_graph
