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
        self.byte_order = "little"
        self.signed = False
        self.encoding = "ASCII"

        # The meta.db header consists of the common .db header and n sections.
        # We're going to do a little set up work, so that's easy to change if 
        # any revisions change the orders.

        # We're are going to specify 2 maps:
        #   - One dictionary maps the section name to an index 
        #       (which follows the order that the sections are seen
        #        in the meta.db header)
        #   - The second dictionary maps the section name to a 
        #       function that reads the section. Each function is defined
        #       as __read_<section_name>_section(self, section_pointer: int, section_size: int) -> None 

        # Here I'm mapping the section name to it's order in the meta.db header
        header_map = {"General Properties": 0, "Identifier Names": 1, "Performance Metrics": 2, \
                        "Context Tree": 3, "Common String Table": 4, "Load Modules": 5, \
                        "Source Files": 6, "Functions": 7}
        
        # Now let's create a function to section map
        reader_map = {"General Properties": self.__read_general_properties_section, \
                        "Common String Table": self.__read_common_string_table_section, \
                        "Source Files": self.__read_source_files_section, \
                        "Functions": self.__read_functions_section, \
                        "Load Modules": self.__read_load_modules_section, \
                        "Context Tree": self.__read_context_tree_section, \
                        "Identifier Names": self.__read_identifier_names_section, \
                        "Performance Metrics": self.__read_performance_metrics_section}
        
        # Another thing thing that we should consider is the order to read the sections.
        # Here is a list of section references (x -> y means x references y)
        #   - "Source Files"    ->  "Common String Table"
        #   - "Functions"       ->  "Common String Table"
        #   - "Context Tree"    ->  "Common String Table"
        #   - "Load Modules"    ->  "Common String Table"
        #   - "Functions"       ->  "Source Files"
        #   - "Context Tree"    ->  "Functions"
        #   - "Context Tree"    ->  "Source Files"
        #   - "Functions"       ->  "Load Modules"
        # 
        # Thus we want to keep this order when reading sections:
        # "Common String Table" -> "Source Files" -> "Functions" -> "Context Tree", and 
        # "Common String Table -> "Load Modules" -> "Context Tree"
        # Here I'm specifying the order of reading the file
        self.read_order = ["Common String Table", "General Properties", "Source Files", \
                            "Load Modules", "Functions", "Context Tree", "Identifier Names", \
                            "Performance Metrics"]


        
        # Let's make sure that we include every section in the read order and reader_map
        assert set(self.read_order) == set(header_map) and set(header_map) == set(reader_map)


        # Now to the actual reading of the meta.db file

        # reading the meta.db header
        self.__read_meta_header()

        # now let's read all the sections
        for section_name in self.read_order:
            section_index = header_map[section_name]
            section_pointer = self.section_pointer[section_index]
            section_size = self.section_size[section_index]
            section_reader = reader_map[section_name]
            section_reader(section_pointer, section_size)



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
        format_identifier = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)

        # next byte (u8) contains the "Common major version, currently 4"
        self.major_version = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed) 
        # next byte (u8) contains the "Specific minor version"
        self.minor_version = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed) 

    def __read_meta_header(self) -> None:
        """ 
        Reads meta.db file header with version 4.0

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-version-40
        """

        # reading the common .db file header
        self.__read_common_header()

        # Reading meta header
        # this header starts at 0x10
        # going to that section just in case
        if self.meta_file.tell() != 0x10:
            self.meta_file.seek(0x10)

        self.section_pointer = []
        self.section_size = []
        # In the header each section is given 16 bytes:
        #   - First 8 bytes specify the total size of the section (in bytes)
        #   - Last 8 bytes specify a pointer to the beggining of the section
        for i in range(len(self.read_order)):
            self.section_size.append(int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed))
            self.section_pointer.append(int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed))

    def __read_general_properties_section(self, section_pointer: int, section_size: int) -> None:
        """Reads the general properties of the trace. 
        Sets:
         
        self.database_title: Title of the database. May be provided by the user.

        self.database_description: Human-readable Markdown description of the database.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-general-properties-section
        """
        
        # go to the right spot in the file
        self.meta_file.seek(section_pointer)
        title_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        description_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)

        self.database_title = self.__read_string(title_pointer)
        self.database_description = self.__read_string(description_pointer)

    def __get_common_string(self, string_pointer: int) -> str:
        """Given the file pointer to find string, returns the string."""
        if string_pointer in self.common_string_index_map:
            return self.common_strings[self.common_string_index_map[string_pointer]]
        else:
            print("Couldn't Find String at Pointer:", string_pointer)
            return None

    def __read_common_string_table_section(self, section_pointer: int, section_size: int) -> None:
        # Let's go to the section
        self.meta_file.seek(section_pointer)
        
        # We know that this section is just a densely packed list of strings,
        # seperated by the null character
        # So to create a list of these strings, we'll read them all into one string then
        # split them by the null character

        # Reading entire section into a string
        total_section: str = str(self.meta_file.read(section_size), encoding='UTF-8')
        
        # Splitting entire section into list of strings
        self.common_strings: list[str] = total_section.split("\0")

        # Now we are creating a map between the original location to the string
        # to the index of the string in self.common_strings.
        # This is because we are passed pointers to find the string in other sections
        pointer_index = section_pointer
        # pointer_index = 0
        self.common_string_index_map: dict = {}
        for i in range(len(self.common_strings)):
            self.common_string_index_map[pointer_index] = i
            pointer_index += (len(self.common_strings[i]) + 1)
        
    def __get_load_modules_index(self, load_module_pointer: int) -> int:
        """
        Given the pointer to where the file would exists in meta.db,
        returns the index of the file in self.source_files_list.
        """
        return ((load_module_pointer - self.load_modules_pointer) // self.load_module_size)

    def __read_load_modules_section(self, section_pointer: int, section_size: int) -> None:
        """
        Reads the "Load Modules" Section of meta.db.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-load-modules-section
        """
        # go to the right spot in meta.db
        self.meta_file.seek(section_pointer)
        
        # Load modules used in this database 
        self.load_modules_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        # Number of load modules listed in this section (u32)
        num_load_modules = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
        # Size of a Load Module Specification, currently 16 (u16)
        self.load_module_size = int.from_bytes(self.meta_file.read(2), byteorder=self.byte_order, signed=self.signed)
        
        
        # Going to store file's path in self.load_modules_list.
        # Each will contain the index of file's path string in 
        # self.common_string
        self.load_modules_list: list[dict] = []
       
        for i in range(num_load_modules):
            current_index = self.load_modules_pointer + (i * self.load_module_size)
            self.meta_file.seek(current_index)

            # Flags -- Reserved for future use (u32)
            flags = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
            # empty space that we need to skip
            self.meta_file.read(4)
            # Full path to the associated application binary
            path_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            module_map = {"string_index": self.common_string_index_map[path_pointer]}
            self.load_modules_list.append(module_map)

    def __read_string(self, file_pointer: int) -> str:
        """ 
        Helper function to read a string from the file starting at the file_pointer
        and ending at the first occurence of the null character
        """
        self.meta_file.seek(file_pointer)
        name = ""
        while True:
            read = str(self.meta_file.read(1), encoding='UTF-8')
            if read == "\0":
                break
            name += read
        return name
            
    def __read_identifier_names_section(self, section_pointer: int, section_size: int) -> None:
        """
        Reads "Identifier Names" Section and Identifier Name strings in self.names_list 

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-hierarchical-identifier-names-section
        """

        # go to correct section of file
        self.meta_file.seek(section_pointer)

        # Human-readable names for Identifier kinds
        names_pointer_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        # Number of names listed in this section
        num_names = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed)

        self.names_list: list[str] = []

        for i in range(num_names):
            self.meta_file.seek(names_pointer_pointer + (i * 8))
            names_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            self.names_list.append(self.__read_string(names_pointer))
    
    def __read_performance_metrics_section(self, section_pointer: int, section_size: int) -> None:
        return
    

    def __get_function_index(self, function_pointer: int) -> int:
        """
        Given the pointer to where the function would exists in meta.db,
        returns the index of the file in self.functions_list.
        """
        index = ((function_pointer - self.functions_array_pointer) // self.function_size)
        assert index < len(self.functions_list)
        return index 
    
    def __read_functions_section(self, section_pointer: int, section_size: int) -> None:
        """
        Reads the "Functions" section of meta.db.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-functions-section
        """
        
        # go to correct section in file
        self.meta_file.seek(section_pointer)

        
        self.functions_array_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        num_functions = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
        self.function_size = int.from_bytes(self.meta_file.read(2), byteorder=self.byte_order, signed=self.signed)
        
        self.functions_list: list[dict] = []
        for i in range(num_functions):
            current_index = self.functions_array_pointer + (i * self.function_size)
            self.meta_file.seek(current_index)
            function_name_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            modules_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            modules_offset = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            file_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            source_line = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
            flags = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
            source_file_index = None
            load_module_index = None
            function_name_index = None
            if function_name_pointer != 0:
                function_name_index = self.common_string_index_map[function_name_pointer]
            if modules_pointer != 0:
                load_module_index = self.__get_load_modules_index(modules_pointer)
                # currently ignoring offset -- no idea how that's used
            if file_pointer != 0:
                source_file_index = self.__get_source_file_index(file_pointer)
            
            current_function_map = {"string_index": function_name_index, \
                                    "source_line": source_line, "load_modules_index": load_module_index, \
                                    "source_file_index": source_file_index}
            self.functions_list.append(current_function_map)
        

    def __get_source_file_index(self, source_file_pointer: int) -> int:
        """
        Given the pointer to where the file would exists in meta.db,
        returns the index of the file in self.source_files_list.
        """
        index = ((source_file_pointer - self.source_files_pointer) // self.source_file_size)
        assert index < len(self.source_files_list)
        return index

    def __read_source_files_section(self, section_pointer: int, section_size: int) -> None:
        """
        Reads the "Source Files" Section of meta.db.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-source-files-section
        """

        self.meta_file.seek(section_pointer)
        
        # Source files used in this database 
        self.source_files_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)

        # Number of source files listed in this section (u32)
        num_files = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
        
        # Size of a Source File Specification, currently 16 (u16)
        self.source_file_size = int.from_bytes(self.meta_file.read(2), byteorder=self.byte_order, signed=self.signed)
        
        # Looping through individual files to get there information now
        self.meta_file.seek(self.source_files_pointer)

        # Going to store file's path in self.files_list.
        # Each will contain the index of file's path string in 
        # self.common_string 
        self.source_files_list: list[dict] = []
        for i in range(num_files):
            # Reading information about each individual source file
            self.meta_file.seek(self.source_files_pointer + (i * self.source_file_size))

            flag = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
            # empty space that we need to skip
            self.meta_file.read(4)
            # Path to the source file. Absolute, or relative to the root database directory.
            # The string pointed to by pPath is completely within the Common String Table section,
            # including the terminating NUL byte.
            file_path_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            string_index = self.common_string_index_map[file_path_pointer]
            source_file_map = {"string_index": string_index}
            self.source_files_list.append(source_file_map)
            
    def __read_context_tree_section(self, section_pointer: int, section_size: int) -> None:
        """
        Reads the "Context Tree" section of meta.db.

        Loops and calls __read_single_entry_point with the correct pointer to read the correct entry and add it to the CCT.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-context-tree-section
        """

        self.cct = Graph()
        self.context_map: dict[int, dict] = {}

        # Reading "Context Tree" section header

        # make sure we're in the right spot of the file
        self.meta_file.seek(section_pointer)

        # ({Entry}[nEntryPoints]*)
        entry_points_array_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        # (u16)
        num_entry_points = int.from_bytes(self.meta_file.read(2), byteorder=self.byte_order, signed=self.signed)
        # (u8)
        entry_point_size = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed)
                
        for i in range (num_entry_points):
            current_pointer = entry_points_array_pointer + (i * entry_point_size)
            self.__read_single_entry_point(current_pointer)

    def __read_single_entry_point(self, entry_point_pointer: int) -> None:
        """
        Reads single (root) context entry.

        Reads the correct entry and adds it to the CCT.

        Documentation: https://gitlab.com/hpctoolkit/hpctoolkit/-/blob/develop/doc/FORMATS.md#metadb-context-tree-section
        """

        self.meta_file.seek(entry_point_pointer)

        # Reading information about child contexts
        # Total size of *pChildren (I call pChildren children_pointer), in bytes (u64)
        children_size = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        # Pointer to the array of child contexts
        children_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)

        # Reading information about this context
        # Unique identifier for this context (u32)
        context_id = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
        # Type of entry point used here (u16)
        entry_point_type = int.from_bytes(self.meta_file.read(2), byteorder=self.byte_order, signed=self.signed)
        # next 2 bytes are blank
        self.meta_file.read(2)
        # Human-readable name for the entry point
        pretty_name_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
        # Create Node for this context
        node: Node = Node(None, None, None)
        node.add_calling_context_id(context_id)
        # Adding the Node to the CCT
        self.cct.add_root(node)
        self.cct.add_to_map(context_id, node)

        # map context for this context 
        context = {"string_index": self.common_string_index_map[pretty_name_pointer]}
        self.context_map[context_id] = context

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
            children_size = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            index += 8
            # Pointer to the array of child contexts
            children_pointer = int.from_bytes(self.meta_file.read(8), byteorder=self.byte_order, signed=self.signed)
            index += 8

            # Reading information about this context
            # Unique identifier for this context (u32)
            context_id = int.from_bytes(self.meta_file.read(4), byteorder=self.byte_order, signed=self.signed)
            index += 4
            # Reading flags (u8)
            flags = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed)
            index += 1
            # Relation this context has with its parent (u8)
            relation = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed)
            index += 1
            # Type of lexical context represented (u8)
            lexical_type = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed)
            index += 1
            # Size of flex, in u8[8] "words" (bytes / 8) (u8)
            num_flex_words = int.from_bytes(self.meta_file.read(1), byteorder=self.byte_order, signed=self.signed)
            index += 1
            # Bitmask for defining propagation scopes (u16) 
            propogation = int.from_bytes(self.meta_file.read(2), byteorder=self.byte_order, signed=self.signed)
            index += 2
            # Empty space
            self.meta_file.read(6)
            index += 6

            # reading flex
            flex = self.meta_file.read(8 * num_flex_words)
            index += (8 * num_flex_words)

            # Creating Node for this context
            node = Node(None, None, parent_node)
            node.add_calling_context_id(context_id)

            # Connecting this node to the parent node
            parent_node.add_child(node)

            # Adding this node to the graph
            self.cct.add_to_map(context_id, node)

            function_index: int = None
            source_file_index: int = None
            source_file_line: int = None
            load_module_index: int = None
            load_module_offset: int = None

            
            # flex is u8[8][num_flex],
            # meaning that one flex word is 8 bytes or u64

            # Bit 0: hasFunction. If 1, the following sub-fields of flex are present:
            #   flex[0]: FS* pFunction: Function associated with this context
            if flags & 1 != 0:
                sub_flex = int.from_bytes(flex[0:8], byteorder=self.byte_order, signed=self.signed)
                flex = flex[8:]
                function_index = self.__get_function_index(sub_flex)
                
            # Bit 1: hasSrcLoc. If 1, the following sub-fields of flex are present:
            #   flex[1]: SFS* pFile: Source file associated with this context
            #   flex[2]: u32 line: Associated source line in pFile
            if flags & 2 != 0:
                sub_flex_1 = int.from_bytes(flex[0:8], byteorder=self.byte_order, signed=self.signed)
                sub_flex_2 = int.from_bytes(flex[8:10], byteorder=self.byte_order, signed=self.signed)
                flex = flex[10:]
                source_file_index = self.__get_source_file_index(sub_flex_1)
                source_file_line = sub_flex_2
                
            # Bit 2: hasPoint. If 1, the following sub-fields of flex are present:
            #   flex[3]: LMS* pModule: Load module associated with this context
            #   flex[4]: u64 offset: Associated byte offset in *pModule
            if flags & 4 != 0:
                sub_flex_1 = int.from_bytes(flex[0:8], byteorder=self.byte_order, signed=self.signed)
                sub_flex_2 = int.from_bytes(flex[8:16], byteorder=self.byte_order, signed=self.signed)
                flex = flex[16:]
                load_module_index = self.__get_load_modules_index(sub_flex_1)
                load_module_offset = sub_flex_2


            # creating a map for this context
            context = {"relation": relation, "lexical_type": lexical_type, \
                       "function_index": function_index, \
                       "source_file_index": source_file_index, \
                       "source_file_line": source_file_line, \
                       "load_module_index":load_module_index, \
                       "load_module_offset": load_module_offset}
            
            self.context_map[context_id] = context

            # recursively call this function to add more children
            return_address = self.meta_file.tell()
            self.__read_children_contexts(children_pointer, children_size, node)
            self.meta_file.seek(return_address)








       



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


def main():
    meta_loc = '/Users/movsesyanae/Programming/Research/pipit_data/hpctoolkit-lulesh2.0-database/meta.db'
    x = MetaReader(meta_loc)

if __name__ == "__main__":
    main()
