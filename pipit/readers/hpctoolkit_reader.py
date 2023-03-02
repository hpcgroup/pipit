# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT


from xml.etree.ElementTree import Element, ElementTree
import pandas as pd
import pipit.trace
from pipit.graph import Graph, Node

class MetaReader:



    def __init__(self, file_location):

        # open the file to ready in binary mode (rb) 
        self.meta_file = open(file_location, "rb")
        
        # setting necessary read options
        self.byte_order = "big"
        self.signed = False
        self.encoding = "ASCII"


        # reading the meta.db header (and rest of the file)
        self.__read_meta_file()





    def __read_common_header(self) -> None:
        """ 
        Reads common .db file header version 4.0
        
        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#common-file-structure
        """

        # read Magic identifier ("HPCPROF-tracedb_")
        # first ten buyes are HPCTOOLKIT in ASCII 
        identifier = str(self.meta_file.read(10), encoding=self.encoding)
        assert(identifier == "HPCTOOLKIT")

        # next 4 bytes (u8) are the "Specific format identifier"
        format_identifier = int.from_bytes(self.meta_file.read(4,  byteorder=self.byte_order, signed=self.signed))

        # next byte (u8) contains the "Common major version, currently 4"
        self.major_version = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed)) 
        # next byte (u8) contains the "Specific minor version"
        self.minor_version = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed)) 

    def __read_meta_file(self) -> None:
        """ 
        Reads meta.db file with version 4.0

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-version-40
        """

        # reading the common .db file header
        self.__read_common_header()

        # Reading meta header
        # this header starts at 0x10
        # going to that section just in case
        if self.meta_file.tell() != 0x10:
            self.meta_file.seek(0x10)
        
        # Reading "General Properties" section
        # Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-general-properties-section
        # First 8 bytes of General Properties is a pointer to the title
        self.title_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
        # Last 8 bytes of General Properties is a pointer to the description
        self.description_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))

        # Reading "Identifier Names" section
        # Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-hierarchical-identifier-names-section
        self.identifier_names_array_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
        self.num_identifier_names = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed))
        # space is left in the file for more information in the future, 
        # so we need to go to the right position for the next read.
        if self.meta_file.tell() != 0x30:
            self.meta_file.seek(0x30)

        # Reading "Performance Metrics" section
        # Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-performance-metrics-section
        # skipping this section for now -- will come back
        self.meta_file.read(16)

        # Reading "Context Tree" section
        self.__read_context_tree_section()
        

        # Reading Common String Table
        # Skipping this section since I can't see how it's stored in the documentation
        self.meta_file.read(16)

        # Reading "Load Modules" section
        # Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-load-modules-section
        # Skipping this section for now  -- not sure if it's necessary

        # Reading "Source Files" section
        self.__read_source_files_section()

    def __read_source_files_section(self) -> None:
        """
        Reads the "Source Files" Section of meta.db.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-source-files-section
        """

        # Reading "Source Files" section header

        # make sure we're in the right spot of the file
        if self.meta_file.tell() != 0x70:
            self.meta_file.seek(0x70)

        # Source files used in this database 
        files_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
        # Number of source files listed in this section (u32)
        num_files = int.from_bytes(self.meta_file.read(4,  byteorder=self.byte_order, signed=self.signed))
        # Size of a Source File Specification, currently 16 (u16)
        file_size = int.from_bytes(self.meta_file.read(4,  byteorder=self.byte_order, signed=self.signed))

        # Looping through individual files to get there information now
        for i in range(num_files):
            # Reading information about each individual source file
            current_index = files_pointer + (i * file_size)
            self.meta_file.seek(current_index)

            flag = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
            # Path to the source file. Absolute, or relative to the root database directory.
            # The string pointed to by pPath is completely within the Common String Table section,
            # including the terminating NUL byte.
            file_path_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))



        

    def __read_context_tree_section(self) -> None:
        """
        Reads the "Context Tree" section of meta.db.

        Loops and calls __read_single_entry_point with the correct pointer to read the correct entry and add it to the CCT.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-context-tree-section
        """

        self.cct = Graph()

        # Reading "Context Tree" section header

        # make sure we're in the right spot of the file
        if self.meta_file.tell() != 0x40:
            self.meta_file.seek(0x40)

        # ({Entry}[nEntryPoints]*)
        entry_points_array_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
        # (u16)
        num_entry_points = int.from_bytes(self.meta_file.read(2,  byteorder=self.byte_order, signed=self.signed))
        # (u8)
        entry_point_size = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed))
                
        for i in range (num_entry_points):
            current_pointer = entry_points_array_pointer + i * entry_point_size
            self.__read_single_entry_point(current_pointer)

        # reading the context tree has us go around the file, so we need to come back to the right spot
        self.meta_file.seek(0x50)  

    def __read_single_entry_point(self, entry_point_pointer: int) -> None:
        """
        Reads single (root) context entry.

        Reads the correct entry and adds it to the CCT.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-context-tree-section
        """

        self.meta_file.seek(entry_point_pointer)

        # Reading information about child contexts
        # Total size of *pChildren (I call pChildren children_pointer), in bytes (u64)
        children_size = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
        # Pointer to the array of child contexts
        children_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))

        # Reading information about this context
        # Unique identifier for this context (u32)
        context_id = int.from_bytes(self.meta_file.read(4,  byteorder=self.byte_order, signed=self.signed))
        # Type of entry point used here (u16)
        entry_point_type = int.from_bytes(self.meta_file.read(2,  byteorder=self.byte_order, signed=self.signed))
        # Human-readable name for the entry point
        pretty_name_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))

        # Create Node for this context
        node: Node = Node(None, None, None)
        node.add_calling_context_id(context_id)
        # Adding the Node to the CCT
        self.cct.add_root(node)
        self.cct.add_to_map(context_id, node)

        # Reading the children contexts
        self.__read_children_contexts(children_pointer, children_size, node)

    def __read_children_contexts(self, context_array_pointer: int, total_size: int, parent_node: Node) -> None:
        """
        Recursive function to read all child contexts and add it to the CCT

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-context-tree-section
        """
        if total_size <= 0 or context_array_pointer <= 0:
            return
        self.meta_file.seek(context_array_pointer)
        index = 0
        while(index < total_size):
            # Reading information about child contexts (as in the children of this context)
            # Total size of *pChildren (I call pChildren children_pointer), in bytes (u64)
            children_size = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
            index += 8
            # Pointer to the array of child contexts
            children_pointer = int.from_bytes(self.meta_file.read(8,  byteorder=self.byte_order, signed=self.signed))
            index += 8

            # Reading information about this context
            # Unique identifier for this context (u32)
            context_id = int.from_bytes(self.meta_file.read(4,  byteorder=self.byte_order, signed=self.signed))
            index += 4
            # Reading flags (u8)
            flags = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed))
            index += 1
            # Relation this context has with its parent (u8)
            relation = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed))
            index += 1
            # Type of lexical context represented (u8)
            lexical_type = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed))
            index += 1
            # Size of flex, in u8[8] "words" (bytes / 8) (u8)
            num_flex_words = int.from_bytes(self.meta_file.read(1,  byteorder=self.byte_order, signed=self.signed))
            index += 1
            # Bitmask for defining propagation scopes (u16) 
            propogation = int.from_bytes(self.meta_file.read(2,  byteorder=self.byte_order, signed=self.signed))
            index += 2
            # Skipping reading flex
            self.meta_file.read(num_flex_words * 8)
            index += (8 * num_flex_words)

            # Creating Node for this context
            node = Node(None, None, parent_node)
            node.add_calling_context_id(context_id)

            # Connecting this node to the parent node
            parent_node.add_child(node)

            # Adding this node to the graph
            self.cct.add_to_map(context_id, node)

            # recursively call this function to add more children
            self.__read_children_contexts(children_pointer, children_size, node)








       



class ExperimentReaderOld:
    def __init__(self, file_location):
        self.tree = ElementTree(file=file_location)
        self._create_identifier_name_table()

    def get_function_name(self, procedure_table_id):
        """return function name, given a procedure_table_id"""

        procedure_table_search = ".//Procedure[@i='" + procedure_table_id + "']"
        procedure = self.tree.find(procedure_table_search)
        return procedure.get("n")

    def _create_identifier_name_table(self):
        self.identifier_name_table = {}
        for identifier in list(list(list(list(self.tree.getroot())[1])[0])[0]):
            identifier_map = identifier.attrib
            self.identifier_name_table[int(identifier_map["i"])] = identifier_map["n"]

    def get_identifier_name(self, kind):
        return self.identifier_name_table[kind]

    def get_min_max_time(self):
        # gets the start and end time of the program

        search = './/TraceDB[@i="0"]'
        e = self.tree.find(search)
        time = (int(e.get("db-min-time")), int(e.get("db-max-time")))
        return time

    def create_graph(self):
        """Traverses through the experiment.xml's SecCallPathProfileData tag,
        creating a Node for every PF tag and adding it to the Graph
        """

        graph = Graph()
        call_path_data = list(list(self.tree.getroot())[-1])[-1]
        root_elems = list(call_path_data)
        for root_elem in root_elems:
            node = self.graph_helper(None, root_elem, graph)
            if node is not None:
                graph.add_root(node)

        return graph

    def graph_helper(self, parent_node: Node, curr_element: Element, graph: Graph):
        """Recursive helper function for creating the graph - if the current
        item in the SecCallPathProfileData is 'PF' add a node, otherwise create
        an association between the id and the last node
        """
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
                parent_node.add_calling_context_id(calling_context_id)
                graph.add_to_map(calling_context_id, parent_node)

            for child_elem in list(curr_element):
                self.graph_helper(parent_node, child_elem, graph)
            return None


class ProfileReader:
    # class to read data from profile.db file

    def __init__(self, file_location, experiment_reader):
        # gets the pi_ptr variable to be able to read the identifier tuples
        self.experiment_reader = experiment_reader

        self.file = open(file_location, "rb")
        file = self.file
        file.seek(32)

        # need to test to see if correct
        byte_order = "big"
        signed = False

        # Profile Info section offset (pi_ptr)
        self.pi_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

    def read_info(self, prof_info_idx):
        """Given a prof_info_id, returns the heirarchal identifier tuples
        associated with it - information such as thread id, mpi_rank,
        node_id, etc.
        """
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
            kind = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
            kind = kind & 0x3FFF
            p_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
            # l_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
            file.read(8)
            identifier_name = self.experiment_reader.get_identifier_name(kind)
            tuples_list.append((identifier_name, p_val))
        return tuples_list


class HPCToolkitReader:
    def __init__(self, dir_name) -> None:
        self.dir_name = dir_name  # directory of hpctoolkit trace files being read

    def read(self):
        """This function reads through the trace.db file with two nested loops -
        The outer loop iterates on the rank of the process while the inner
        loop Iterates through each trace line for the given rank and adds it
        to the dictionary
        """
        dir_location = self.dir_name  # directory of hpctoolkit trace files being read

        # open file
        file = open(dir_location + "/trace.db", "rb")

        experiment_reader = ExperimentReader(dir_location + "/experiment.xml")

        profile_reader = ProfileReader(dir_location + "/profile.db", experiment_reader)

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
            "Timestamp (ns)": [],
            "Event Type": [],
            "Name": [],
            "Thread": [],
            "Process": [],
            "Host": [],
            "Node": [],
        }
        min_max_time = experiment_reader.get_min_max_time()

        # cycle through trace headers (each iteration in this outer loop is a seperate
        # process/thread/rank)
        for i in range(0, hdr_size, 22):
            # proc_num = int(i / 22)
            file.seek(hdr_ptr + i)

            # prof_info_idx (in profile.db)
            prof_info_idx = int.from_bytes(
                file.read(4), byteorder=byte_order, signed=signed
            )
            proc_num = profile_reader.read_info(prof_info_idx)
            # file.read(4)

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

                    node = graph.get_node(calling_context_id)  # the node in the Graph

                    # closing functions exited
                    close_node = last_node
                    intersect_level = -1
                    intersect_node = node.get_intersection(last_node)
                    # this is the highest node that last_node and node have in
                    # common we want to close every enter time event higher
                    # than than interest node, because those functions have
                    # exited

                    if intersect_node is not None:
                        intersect_level = intersect_node.get_level()
                    while (
                        close_node is not None
                        and close_node.get_level() > intersect_level
                    ):
                        data["Name"].append(close_node.name)
                        data["Event Type"].append("Leave")
                        data["Timestamp (ns)"].append(timestamp)
                        data["Process"].append(proc_num[1][1])
                        data["Thread"].append(proc_num[2][1])
                        data["Host"].append(proc_num[0][1])
                        data["Node"].append(close_node)
                        close_node = close_node.parent

                    # creating new rows for the new functions entered
                    enter_list = node.get_node_list(intersect_level)
                    # the list of nodes higher than interesect_level
                    # (the level of interesect_node)

                    # all of the nodes in this list have entered into the
                    # function since the last poll so we want to create entries
                    # in the data for the Enter event
                    for enter_node in enter_list[::-1]:
                        data["Name"].append(enter_node.name)
                        data["Event Type"].append("Enter")
                        data["Timestamp (ns)"].append(timestamp)
                        data["Process"].append(proc_num[1][1])
                        data["Thread"].append(proc_num[2][1])
                        data["Host"].append(proc_num[0][1])
                        data["Node"].append(enter_node)
                    last_node = node

                last_id = calling_context_id  # updating last_id

            # adding last data for trace df
            close_node = last_node

            # after reading through all the trace lines, some Enter events will
            # not have matching Leave events, as the functions were still
            # running in the last poll.  Here we are adding Leave events to all
            # of the remaining unmatched Enter events
            while close_node is not None:
                data["Name"].append(close_node.name)
                data["Event Type"].append("Leave")
                data["Timestamp (ns)"].append(min_max_time[1] - min_max_time[0])
                data["Process"].append(proc_num[1][1])
                data["Thread"].append(proc_num[2][1])
                data["Host"].append(proc_num[0][1])
                data["Node"].append(close_node)
                close_node = close_node.parent

        trace_df = pd.DataFrame(data)
        # Need to sort df by timestamp then index
        # (since many events occur at the same timestamp)

        # rename the index axis, so we can sort with it
        trace_df.rename_axis("index", inplace=True)

        # sort by timestamp then index
        trace_df.sort_values(
            by=["Timestamp (ns)", "index"],
            axis=0,
            ascending=True,
            inplace=True,
            ignore_index=True,
        )

        trace_df = trace_df.astype(
            {
                "Event Type": "category",
                "Name": "category",
                "Thread": "category",
                "Process": "category",
                "Host": "category",
            }
        )

        self.trace_df = trace_df
        return pipit.trace.Trace(None, trace_df)
