# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland.  See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from pipit.graph import Graph, Node


def create_cct(events):
    """
    Generic function to iterate through the events dataframe and create a CCT.
    Uses pipit's graph data structure for this. Returns a CCT
    and creates a new column in the Events DataFrame that stores
    a reference to each row's corresponding node in the CCT.
    """

    # CCT and list of nodes in DataFrame
    graph = Graph()
    graph_nodes = [None for i in range(len(events))]

    # determines whether a node exists or not
    callpath_to_node = dict()

    node_id = 0  # each node has a unique id

    # Filter the DataFrame to only Enter/Leave
    enter_leave_df = events.loc[events["Event Type"].isin(["Enter", "Leave"])]

    # list of processes and/or threads to iterate over
    if "Thread" in events.columns:
        exec_locations = set(zip(events["Process"], events["Thread"]))
        has_thread = True
    else:
        exec_locations = set(events["Process"])
        has_thread = False

    for curr_loc in exec_locations:
        # only filter by thread if the trace has a thread column
        if has_thread:
            curr_process, curr_thread = curr_loc
            filtered_df = enter_leave_df.loc[
                (enter_leave_df["Process"] == curr_process)
                & (enter_leave_df["Thread"] == curr_thread)
            ]
        else:
            filtered_df = enter_leave_df.loc[(enter_leave_df["Process"] == curr_loc)]

        curr_depth, callpath = 0, ""

        """
        Iterating over lists instead of
        DataFrame columns is more efficient
        """
        df_indices = filtered_df.index.to_list()
        function_names = filtered_df["Name"].to_list()
        event_types = filtered_df["Event Type"].to_list()

        # stacks used to iterate through the trace and add nodes to the cct
        functions_stack, nodes_stack = [], []

        # iterating over the events of the current thread's trace
        for i in range(len(filtered_df)):
            curr_df_index, evt_type, function_name = (
                df_indices[i],
                event_types[i],
                function_names[i],
            )

            # encounter a new function through its entry point.
            if evt_type == "Enter":
                # add the function to the stack and get the call path
                functions_stack.append(function_name)
                callpath = "->".join(functions_stack)

                # get the parent node of the function if it exists
                parent_node = None if curr_depth == 0 else nodes_stack[-1]

                if callpath in callpath_to_node:
                    # don't create new node if callpath is in map
                    curr_node = callpath_to_node[callpath]
                else:
                    # create new node if callpath isn't in map
                    curr_node = Node(node_id, parent_node, curr_depth)
                    callpath_to_node[callpath] = curr_node
                    node_id += 1

                    # add node as root or child of its
                    # parent depending on current depth
                    graph.add_root(
                        curr_node
                    ) if curr_depth == 0 else parent_node.add_child(curr_node)

                # Update nodes stack, column, and current depth
                nodes_stack.append(curr_node)
                graph_nodes[curr_df_index] = curr_node
                curr_depth += 1
            else:
                """
                Pop node from top of stack once you
                encounter the Leave event for a function
                """
                nodes_stack.pop()

                # Update functions stack and current depth
                functions_stack.pop()
                curr_depth -= 1
                

    # Update the Trace with the generated cct
    events["Graph_Node"] = graph_nodes

    return graph
