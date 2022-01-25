from xml.etree.ElementTree import ElementTree


class ExperimentReader:
    
    
    def __init__(self, file_location):
        self.tree = ElementTree(file = file_location)

    def getName(self, metric_id):
        search = './/Procedure[@i=\'' + str(metric_id) + '\']'
        e = self.tree.find(search)
        return e.get('n')
        


class ProfileReader:


    def __init__(self, file_location):
        self.file = open(file_location, "rb")
        file = self.file
        file.seek(32)
        
        # need to test to see if correct
        byte_order = 'big'
        signed = False



        # Profile Info section offset (pi_ptr)
        
        self.pi_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

    def read_info(self, prof_info_idx):
        byte_order = 'big'
        signed = True
        file = self.file
        

        # Profile Info
        file.seek(self.pi_ptr + (prof_info_idx * 52))
        idt_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        
        #skipping because not in use 
        file.read(24) 

        num_vals = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        num_nzctxs = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
        prof_off = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        print('num_vals', num_vals)


        # Hierarchical Identifier Tuple
        file.seek(idt_ptr)
        kind = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
        p_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        l_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        return (kind, p_val, l_val)

        


    




def read_header(dir_location):

    # open file
    file = open(dir_location + "trace.db", "rb")

    experiment_reader = ExperimentReader(dir_location + "experiment.xml")
    profile_reader = ProfileReader(dir_location + "profile.db")
    
    # read Magic identifier ("HPCPROF-tracedb_")
    encoding = 'ASCII' #idk just guessing rn
    identifier = str(file.read(16), encoding)
    
    # read version
    version_major = file.read(1)
    version_minor = file.read(1)

    # need to test to see if correct
    byte_order = 'big'
    signed = False

    # Number of trace lines (num_traces)
    num_traces = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
    print("num_traces", num_traces)

    # Number of sections (num_sec)
    num_sections = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
    print("num_sections", num_sections)

    # Trace Header section size (hdr_size)
    hdr_size = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
    
    # Trace Header section offset (hdr_ptr)
    hdr_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
    print("hdr_size: ", hdr_size)

    # cycle through trace headers/lines 
    for i in range(0, hdr_size, 22):
        file.seek(hdr_ptr + i)
        # prof_info_idx (in profile.db)
        prof_info_idx = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
        print("prof_info_idx: ", prof_info_idx)

        # printing data from profile.db (kind, physical_value, logical_value)
        print(profile_reader.read_info(prof_info_idx))

        # Trace type
        trace_type = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)

        # Offset of Trace Line start (line_ptr) 
        line_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
        
        # Offset of Trace Line one-after-end (line_end) 
        line_end = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

        print("pls", line_end - line_ptr)

        for j in range (line_ptr, line_end, 12):
            file.seek(j)
            # Timestamp (nanoseconds since epoch)
            timestamp = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
            print("timestamp", timestamp)

            # Sample calling context id (in experiment.xml)
            calling_context_id = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
            print("calling_context_id", calling_context_id)
            # can use this to get name of function from experiement.xml Procedure tab

        

        
file_loc = "../data/ping-pong-database-smaller/" # HPCToolKit database location 
read_header(file_loc)


        
    

