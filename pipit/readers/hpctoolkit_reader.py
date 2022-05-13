from xml.etree.ElementTree import Element, ElementTree
import pandas as pd
import pipit.tracedata
from Graph import Graph, Node


class ExperimentReader:
    def __init__(self, file_location):
        self.tree = ElementTree(file=file_location)

    def get_function_name(self, procedure_table_id):
        # return function name, given a procedure_table_id

        procedure_table_search = ".//Procedure[@i='" + procedure_table_id + "']"
        procedure = self.tree.find(procedure_table_search)
        return procedure.get("n")

    def get_min_max_time(self):
        # gets the start and end time of the program

        search = './/TraceDB[@i="0"]'
        e = self.tree.find(search)
        time = (int(e.get("db-min-time")), int(e.get("db-max-time")))
        return time

    def create_graph(self):
        # Traverses through the experiment.xml's SecCallPathProfileData tag, creating a
        # Node for every PF tag and adding it to the Graph
        graph = Graph()
        call_path_data = list(list(self.tree.getroot())[-1])[-1]
        root_elems = list(call_path_data)
        for root_elem in root_elems:
            node = self.graph_helper(None, root_elem, graph)
            if node is not None:
                graph.add_root(node)

        return graph

    def graph_helper(self, parent_node: Node, curr_element: Element, graph: Graph):
        # Recursive helper function for creating the graph - if the current item in the
        # SecCallPathProfileData is 'PF' add a node, otherwise create an association
        # between the id and the last node
        if curr_element.tag == "PF":
            procedure_table_id = curr_element.attrib["n"]
            function_name = self.get_function_name(procedure_table_id)
            new_node = Node(procedure_table_id, function_name, parent_node)
            if parent_node is not None:
                parent_node.add_child(new_node)

            for child_elem in list(curr_element):
                self.graph_helper(new_node, child_elem, graph)
            return new_node

        else:
            calling_context_id = curr_element.attrib.get("it")
            if calling_context_id is not None:
                parent_node.add_calling_contex_id(calling_context_id)
                graph.add_to_map(calling_context_id, parent_node)

            for child_elem in list(curr_element):
                self.graph_helper(parent_node, child_elem, graph)
            return None


class ProfileReader:
    # class to read data from profile.db file

    def __init__(self, file_location):
        # gets the pi_ptr variable to be able to read the identifier tuples

        self.file = open(file_location, "rb")
        file = self.file
        file.seek(32)

        # need to test to see if correct
        byte_order = "big"
        signed = False

        # Profile Info section offset (pi_ptr)

        self.pi_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

        # To read through all the Hierarchical Identifier Tuples
        # print("Testing")
        # idt_size = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        # print('idt_size', idt_size)
        # idt_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        # print('idt_ptr', idt_ptr)

        # file.seek(idt_ptr)

        # i = 0
        # while i < idt_size:
        #     print("Location: ", i + idt_ptr)
        # num_tuples = int.from_bytes(file.read(2), byteorder=byte_order,
        # signed=signed)
        #     print('num_tuples', num_tuples)
        #     tuples_list = []
        #     for j in range(0, num_tuples, 1):
        # kind = int.from_bytes(file.read(2), byteorder=byte_order,
        # signed=signed)
        # p_val = int.from_bytes(file.read(8), byteorder=byte_order,
        # signed=signed)
        # l_val = int.from_bytes(file.read(8), byteorder=byte_order,
        # signed=signed)
        # tuples_list.append((kind, p_val, l_val))
        #     print(tuples_list)
        #     i += 2 + 18 * num_tuples

        # print("end", i, idt_size)
        # print("End Testing")

    def read_info(self, prof_info_idx):
        # Given a prof_info_id, returns the heirarchal identifier tuples associated
        # with it - information such as thread id, mpi_rank, node_id, etc.
        print("prof_info_idx: ", prof_info_idx)
        byte_order = "big"
        signed = False
        file = self.file

        # Profile Info
        file.seek(self.pi_ptr + (prof_info_idx * 52))
        idt_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

        # Hierarchical Identifier Tuple
        file.seek(idt_ptr)
        num_tuples = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
        tuples_list = []
        for i in range(0, num_tuples, 1):
            # not working - I don't know why, but the second 2 tuples are just incorrect
            kind = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
            p_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
            l_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
            tuples_list.append((kind, p_val, l_val))
        return tuples_list


class HPCToolkitReader:
    def __init__(self, dir_name) -> None:
        if dir_name[-1] != "/":
            dir_name += "/"
        self.dir_name = dir_name  # directory of hpctoolkit trace files being read

    def read(self):
        # This function reads through the trace.db file with two nested loops -
        # The outer loop iterates on the rank of the process while the inner loop
        # Iterates through each trace line for the given rank and adds it to
        # the dictionary

        dir_location = self.dir_name  # directory of hpctoolkit trace files being read

        # open file
        file = open(dir_location + "trace.db", "rb")

        experiment_reader = ExperimentReader(dir_location + "experiment.xml")

        # Not currently in use because getting incorrect data from Identifier Tuples
        # profile_reader = ProfileReader(dir_location + "profile.db")

        # create graph
        graph = experiment_reader.create_graph()

        # read Magic identifier ("HPCPROF-tracedb_")
        # encoding = "ASCII"  # idk just guessing rn
        # identifier = str(file.read(16), encoding)
        file.read(16)

        # read version
        # version_major = file.read(1)
        # version_minor = file.read(1)
        file.read(2)

        # need to test to see if correct
        byte_order = "big"
        signed = False

        # Number of trace lines (num_traces)
        # num_traces = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
        file.read(4)

        # Number of sections (num_sec)
        # num_sections = int.from_bytes(file.read(2), byteorder=byte_order,
        # signed=signed)
        file.read(2)

        # Trace Header section size (hdr_size)
        hdr_size = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

        # Trace Header section offset (hdr_ptr)
        hdr_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        # print("hdr_size: ", hdr_size)

        data = {
            "Function Name": [],
            "Event Type": [],
            "Time": [],
            "Process": [],
            "Graph_Node": [],
            "Level": [],
        }
        min_max_time = experiment_reader.get_min_max_time()

        # cycle through trace headers (each iteration in this outer loop is a seperate
        # process/thread/rank)
        for i in range(0, hdr_size, 22):
            proc_num = int(i / 22)
            file.seek(hdr_ptr + i)

            # prof_info_idx (in profile.db)
            # prof_info_idx = int.from_bytes(
            #     file.read(4), byteorder=byte_order, signed=signed
            # )
            # thread_info = profile_reader.read_info(prof_info_idx)
            file.read(4)

            # Trace type
            # trace_type = int.from_bytes(
            #     file.read(2), byteorder=byte_order, signed=signed
            # )
            file.read(2)

            # Offset of Trace Line start (line_ptr)
            line_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

            # Offset of Trace Line one-after-end (line_end)
            line_end = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

            last_id = -1

            last_node = None

            # iterate through every trace event in the process
            for j in range(line_ptr, line_end, 12):
                file.seek(j)

                # Timestamp (nanoseconds since epoch)
                timestamp = (
                    int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
                    - min_max_time[0]
                )

                # Sample calling context id (in experiment.xml)
                # can use this to get name of function from experiement.xml
                # Procedure tab
                calling_context_id = int.from_bytes(
                    file.read(4), byteorder=byte_order, signed=signed
                )

                # checking to see if the last event wasn't the same
                # if it was then we skip it, as to not have multiple sets of
                # open/close events for a function that it's still in
                if last_id != calling_context_id:

                    # updating the trace_db

                    node = graph.get_node(calling_context_id)  # the node in the CCT

                    # closing functions exited
                    close_node = last_node
                    intersect_level = -1
                    intersect_node = node.get_intersection(last_node)
                    # this is the highest node that last_node and node have in common
                    # we want to close every enter time event higher than than interest
                    # node, because those functions have exited

                    if intersect_node is not None:
                        intersect_level = intersect_node.get_level()
                    while (
                        close_node is not None
                        and close_node.get_level() > intersect_level
                    ):
                        data["Function Name"].append(close_node.name)
                        data["Event Type"].append("Exit")
                        data["Time"].append(timestamp)
                        data["Process"].append(proc_num)
                        data["Graph_Node"].append(close_node)
                        data["Level"].append(close_node.get_level())
                        close_node = close_node.parent

                    # creating new rows for the new functions entered
                    enter_list = node.get_node_list(intersect_level)
                    # the list of nodes higher than interesect_level
                    # (the level of interesect_node)

                    # all of the nodes in this list have entered into the function since
                    # the last poll so we want to create entries in the data for the
                    # Enter event
                    for enter_node in enter_list[::-1]:
                        data["Function Name"].append(enter_node.name)
                        data["Event Type"].append("Enter")
                        data["Time"].append(timestamp)
                        data["Process"].append(proc_num)
                        data["Graph_Node"].append(enter_node)
                        data["Level"].append(enter_node.get_level())
                    last_node = node

                last_id = calling_context_id  # updating last_id

            # adding last data for trace df
            close_node = last_node

            # after reading through all the trace lines, some Enter events will not have
            # matching Exit events,
            # as the functions were still running in the last poll.
            # Here we are adding Exit events to all of the remaining unmatched
            # Enter events
            while close_node is not None:
                data["Function Name"].append(close_node.name)
                data["Event Type"].append("Exit")
                data["Time"].append(min_max_time[1] - min_max_time[0])
                data["Process"].append(proc_num)
                data["Graph_Node"].append(close_node)
                data["Level"].append(close_node.get_level())
                close_node = close_node.parent

        trace_df = pd.DataFrame(data)
        self.trace_df = trace_df
        return pipit.tracedata.TraceData(trace_df)
