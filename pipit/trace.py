# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
from pipit.graph import Graph, Node


class Trace:
    """
    A trace dataset is read into an object of this type, which
    includes one or more dataframes and a calling context tree.
    """

    def __init__(self, definitions, events, cct):
        """Create a new Trace object."""
        self.definitions = definitions
        self.events = events
        self.cct = cct

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

    def create_cct(self):
        """
        Generic function to iterate through the trace events and create a CCT.
        Uses pipit's graph data structure for this. Populates the trace's CCT
        field and creates a new column in the trace's Events DataFrame that stores
        a reference to each row's corresponding node in the CCT.
        """

        # only create the cct if it doesn't exist already
        if self.cct is None:
            # CCT and list of nodes in DataFrame
            graph = Graph()
            graph_nodes = [None for i in range(len(self.events))]

            # determines whether a node exists or not
            callpath_to_node = dict()

            node_id = 0  # each node has a unique id

            # Filter the DataFrame to only Enter/Leave
            enter_leave_df = self.events.loc[
                self.events["Event Type"].isin(["Enter", "Leave"])
            ]

            # Iterating over process & threads to
            # read call stack sequentially
            for process in set(enter_leave_df["Process"]):
                curr_process_df = enter_leave_df.loc[
                    enter_leave_df["Process"] == process
                ]

                for thread in set(curr_process_df["Thread"]):
                    # filter by both process and thread
                    filtered_df = curr_process_df.loc[
                        curr_process_df["Thread"] == thread
                    ]

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
                                curr_node = Node(
                                    node_id, function_name, parent_node, curr_depth
                                )
                                callpath_to_node[callpath] = curr_node
                                node_id += 1

                                # add node as root or child of its
                                # parent depending on current depth
                                graph.add_root(
                                    curr_node
                                ) if curr_depth == 0 else parent_node.add_child(
                                    curr_node
                                )

                            # Update nodes stack, column, and current depth
                            nodes_stack.append(curr_node)
                            graph_nodes[curr_df_index] = curr_node
                            curr_depth += 1
                        else:
                            """
                            Get the corresponding node from top of stack
                            once you encounter the Leave event for a function
                            """
                            curr_node = nodes_stack.pop()

                            # do we want to store node reference in leave row too?
                            graph_nodes[curr_df_index] = curr_node

                            # Update functions stack and current depth
                            functions_stack.pop()
                            curr_depth -= 1

            # Update the Trace with the generated cct
            self.events["Graph_Node"] = graph_nodes
            self.cct = graph
