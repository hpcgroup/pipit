import gzip
import pandas
import pipit.trace

class ProjectionsConstants:
        # Message Creation po
    CREATION                 = 1;

    BEGIN_PROCESSING         = 2;
    END_PROCESSING           = 3;
    ENQUEUE                  = 4;
    DEQUEUE                  = 5;
    BEGIN_COMPUTATION        = 6;
    END_COMPUTATION          = 7;

    BEGIN_INTERRUPT          = 8;
    END_INTERRUPT            = 9;
    MESSAGE_RECV             = 10;
    BEGIN_TRACE              = 11;
    END_TRACE                = 12;
    USER_EVENT               = 13;
    BEGIN_IDLE               = 14;
    END_IDLE                 = 15;
    BEGIN_PACK               = 16;
    END_PACK                 = 17;
    BEGIN_UNPACK             = 18;
    END_UNPACK               = 19;
    CREATION_BCAST           = 20;

    CREATION_MULTICAST       = 21;

    # A record for a user supplied integer value, likely a timestep 
    USER_SUPPLIED            = 26;

    # A record for the memory usage 
    MEMORY_USAGE            = 27;

    # A record for a user supplied string 
    USER_SUPPLIED_NOTE            = 28;
    USER_SUPPLIED_BRACKETED_NOTE            = 29;


    BEGIN_USER_EVENT_PAIR    = 98;
    END_USER_EVENT_PAIR      = 99;
    USER_EVENT_PAIR          = 100;
    USER_STAT 		 = 32;
    # *** USER category *** 
    NEW_CHARE_MSG            = 0;
    #NEW_CHARE_NO_BALANCE_MSG = 1;
    FOR_CHARE_MSG            = 2;
    BOC_INIT_MSG             = 3;
    #BOC_MSG                  = 4;
    #TERMINATE_TO_ZERO        = 5;  # never used ??
    #TERMINATE_SYS            = 6;  # never used ??
    #INIT_COUNT_MSG           = 7;
    #READ_VAR_MSG             = 8;
    #READ_MSG_MSG             = 9;
    #BROADCAST_BOC_MSG        = 10;
    #DYNAMIC_BOC_INIT_MSG     = 11;

    # *** IMMEDIATE category *** 
    LDB_MSG                  = 12;
    #VID_SEND_OVER_MSG        = 13;
    QD_BOC_MSG               = 14;
    QD_BROADCAST_BOC_MSG     = 15;
    #IMM_BOC_MSG              = 16;
    #IMM_BROADCAST_BOC_MSG    = 17;
    #INIT_BARRIER_PHASE_1     = 18;
    #INIT_BARRIER_PHASE_2     = 19;

class STSReader:

    # In 'self.chares', each entry stores (chare_name: str, dimension: int)
    # In 'self.entries', each entry stores (entry_name: str, chare_id: int)

    def __init__(self, file_location):
        self.sts_file = open(file_location, 'r')
        # self.chares = {}
        self.entries = {}
        # self.messages = []
        self.user_events = {}
        self.user_stats = {}
        self.read_sts_file()

    # to get name of entry print <name of chare + :: + name of entry>> 
    def get_entry_name(self, entry_id):
        # self.entries[entry_id][1] is the chare_id (index for self.chares)
        return self.chares[self.entries[entry_id][1]][0] + '::' + self.entries[entry_id][0]

    def get_dimension(self, entry_id):
        # self.entries[entry_id][1] is the chare_id (index for self.chares)
        return self.chares[self.entries[entry_id][1]][1]

    def get_user_event(self, user_event_id):
        return self.user_events[user_event_id]

    def get_user_stat(self, user_event_id):
        return self.user_stats[user_event_id]

        #     public int getNumPerfCounts() {
	# if (hasPAPI) {
	#     return numPapiEvents;
	# } else {
	#     return 0;
	# }

    # unsure what this is used for, but necessary to read PROCESSING
    def get_num_perf_counts(self):
        if hasattr(self, 'papi_event_names'):
            return len(self.papi_event_names)
        else:
            return 0

    def get_event_name(self, event_id):
        return self.user_events[event_id]


    def read_sts_file(self):
        for line in self.sts_file:
            line_arr = line.split()

            # Note: I'm disregarding TOTAL_STATS and TOTAL_EVENTS, because 
            #   projections reader disregards them

            # Note: currently not reading/storing VERSION, MACHINE, SMPMODE, 
            #   COMMANDLINE, CHARMVERSION, USERNAME, HOSTNAME
            
            # create chares array
            if line_arr[0] == 'TOTAL_CHARES':
                total_chares = int(line_arr[1])
                self.chares = [None] * total_chares

            elif line_arr[0] == 'TOTAL_EPS':
                self.num_eps = int(line_arr[1])

            # get num processors
            elif line_arr[0] == 'PROCESSORS':
                self.num_pes = int(line_arr[1])
            
            # create message array
            elif line_arr[0] == 'TOTAL_MSGS':
                total_messages = int(line_arr[1])
                self.message_table = [None] * total_messages
            elif line_arr[0] == 'TIMESTAMP':
                self.timestamp_string = line_arr[1]

            elif line_arr[0] == 'CHARE':
                id = int(line_arr[1])
                name = line_arr[2][1:len(line_arr[2]) - 1]
                dimensions = int(line_arr[3])
                self.chares[id] = (name, dimensions)
                # print(int(line_arr[1]), line_arr[2][1:len(line_arr[2]) - 1])
                
            elif line_arr[0] == 'ENTRY':

                # Need to concat entry_name
                while not line_arr[3].endswith('"'):
                    line_arr[3] = line_arr[3] + ' ' + line_arr[4]
                    del line_arr[4]

                
                id = int(line_arr[2])
                entry_name = line_arr[3][1:len(line_arr[3]) - 1]
                chare_id = int(line_arr[4])
                # name = self.chares[chare_id][0] + '::' + entry_name
                self.entries[id] = (entry_name, chare_id)
        
            elif line_arr[0] == 'MESSAGE':
                id = int(line_arr[1])
                message_size = int(line_arr[2])
                self.message_table[id] = message_size
            
            elif line_arr[0] == 'EVENT':
                id = int(line_arr[1])
                event_name = ''
                # rest of line is the event name
                for i in range(2, len(line_arr)):
                    event_name = event_name + line_arr[i] + ' '
                self.user_events[id] = event_name
            
            elif line_arr[0] == 'STAT':
                id = int(line_arr[1])
                event_name = ''
                # rest of line is the stat
                for i in range(2, len(line_arr)):
                    event_name = event_name + line_arr[i] + ' '
                self.user_events[id] = event_name

            # create papi array
            elif line_arr[0] == 'TOTAL_PAPI_EVENTS':
                num_papi_events = int(line_arr[1])
                self.papi_event_names = [None] * num_papi_events
            
            elif line_arr[0] == 'PAPI_EVENT':
                id = int(line_arr[1])
                papi_event = line_arr[2]
                self.papi_event_names[id] = papi_event

        self.sts_file.close()

            

class ProjectionsReader:
    def __init__(self, executable_location: str) -> None:
        self.executable_location = executable_location
        self.sts_reader = STSReader(self.executable_location + '.prj.sts')
        self.num_pes = self.sts_reader.num_pes

    @staticmethod
    def __create_empty_dict() -> dict:
        return {
            "Function Name": [],
            "Event Type": [],
            "Time": [],
            "Process": [],
            'Details': [],
            'Created By': []
        }

    def read_projections(self):

        if self.num_pes < 1:
            return None
        
        dataframes_list = []
        for i in range(self.num_pes):
            dataframes_list.append(self.__read_log_file(i))

        trace_df = pandas.concat(dataframes_list, ignore_index=True)
        return pipit.trace.Trace(None, trace_df)
    
    def __read_log_file(self, pe_num: int) -> pandas.DataFrame:
        
        sts_reader = self.sts_reader
        
        data = self.__create_empty_dict()
        
        log_file = gzip.open(self.executable_location + '.prj.' + str(pe_num) + '.log.gz', 'rt')
        

        # Basing read on projections log reader and log entry viewer
        for line in log_file:
            line_arr = line.split()

            if not line_arr[0].isnumeric():
                pass
            
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_IDLE:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Idle')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')

                
            elif int(line_arr[0]) == ProjectionsConstants.END_IDLE:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Idle')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')

                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_PACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Pack')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.END_PACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Pack')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_UNPACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Unpack')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.END_UNPACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])

                data['Function Name'].append('Unpack')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.USER_SUPPLIED:
                user_supplied = line_arr[1]
                
                data['Function Name'].append('User Supplied')
                data['Event Type'].append('User Supplied')
                data['Time'].append(-1)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(user_supplied)

            elif int(line_arr[0]) == ProjectionsConstants.USER_SUPPLIED_NOTE:
                time = line_arr[1]
                note = ''
                for i in range(2, len(line_arr)):
                    note = note + line_arr[i] + ' '
                
                data['Function Name'].append('User Supplied Note')
                data['Event Type'].append('User Supplied Note')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(note)
                
            elif int(line_arr[0]) == ProjectionsConstants.USER_SUPPLIED_BRACKETED_NOTE:
                time = line_arr[1]
                end_time = line_arr[2]
                user_event_id = line_arr[3]
                note = 'Event Name: "' + sts_reader.get_event_name(user_event_id) + '" Note: "'
                for i in range(4, len(line_arr)):
                    note = note + line_arr[i] + ' '
                note = note + '"'
                
                data['Function Name'].append('User Supplied Bracketed Note')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(note)
                
                data['Function Name'].append('User Supplied Bracketed Note')
                data['Event Type'].append('Exit')
                data['Time'].append(end_time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(note)
                
                
            elif int(line_arr[0]) == ProjectionsConstants.MEMORY_USAGE:
                memory_usage = int(line_arr[1])
                time = int(line_arr[2])

                data['Function Name'].append('Memory Usage')
                data['Event Type'].append('Memory Usage')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(memory_usage)
                
            elif int(line_arr[0]) == ProjectionsConstants.CREATION:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                send_time = int(line_arr[7])
                sts_reader.get_entry_name(entry)

                data['Function Name'].append('Create')
                data['Event Type'].append('Create')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(sts_reader.get_entry_name(entry))
                
            elif int(line_arr[0]) == ProjectionsConstants.CREATION_MULTICAST:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                send_time = int(line_arr[7])
                numPEs = int(line_arr[8])
                destPEs = []
                for i in (0, numPEs):
                    destPEs.append(int(line_arr[9 + i]))
                
                data['Function Name'].append('Multicast')
                data['Event Type'].append('Multicast')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('To ' + str(numPEs) + 'processors')

                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_PROCESSING:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                recv_time = int(line_arr[7])
                dimensions = sts_reader.get_dimension(entry)
                id = []
                for i in range(8, 8 + dimensions):
                    id.append(int(line_arr[i]))
                cpu_start_time = int(line_arr[8 + dimensions])

                num_perf_counts = sts_reader.get_num_perf_counts()
                perf_counts = []
                for i in range(9 + dimensions, 9 + dimensions + num_perf_counts):
                    perf_counts.append(int(line_arr[i]))
                
                data['Function Name'].append('Processing')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(sts_reader.get_entry_name(entry))
                
            elif int(line_arr[0]) == ProjectionsConstants.END_PROCESSING:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                cpu_end_time = int(line_arr[7])
                num_perf_counts = sts_reader.get_num_perf_counts()
                perf_counts = []
                for i in range(8 + dimensions, 8 + dimensions + num_perf_counts):
                    perf_counts.append(int(line_arr[i]))
                
                data['Function Name'].append('Processing')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(sts_reader.get_entry_name(entry))
                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_TRACE:
                time = int(line_arr[1])

                data['Function Name'].append('Begin Trace')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')

            elif int(line_arr[0]) == ProjectionsConstants.END_TRACE:
                time = int(line_arr[1])

                data['Function Name'].append('Begin Trace')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.MESSAGE_RECV:
                mtype = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                message_length = int(line_arr[5])
                
            elif int(line_arr[0]) == ProjectionsConstants.ENQUEUE:
                mtype = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                
                data['Function Name'].append('Enque')
                data['Event Type'].append('Enque')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('message received from processor ' + pe + ' destined for UNKOWN')

            elif int(line_arr[0]) == ProjectionsConstants.DEQUEUE:
                mtype = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                
                data['Function Name'].append('Deque')
                data['Event Type'].append('Deque')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_INTERRUPT:
                time = int(line_arr[1])
                event = int(line_arr[2])
                pe = int(line_arr[3])

                data['Function Name'].append('Interrupt')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.END_INTERRUPT:
                time = int(line_arr[1])
                event = int(line_arr[2])
                pe = int(line_arr[3])

                data['Function Name'].append('Interrupt')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_COMPUTATION:
                time = int(line_arr[1])
                
                data['Function Name'].append('Computation')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.END_COMPUTATION:
                time = int(line_arr[1])
                
                data['Function Name'].append('Computation')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')
                
            elif int(line_arr[0]) == ProjectionsConstants.USER_EVENT:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])

                details = sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event')
                data['Event Type'].append('User Event')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == ProjectionsConstants.USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event Pair')
                data['Event Type'].append('User Event Pair')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == ProjectionsConstants.BEGIN_USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event Pair')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == ProjectionsConstants.END_USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event Pair')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == ProjectionsConstants.USER_STAT:
                time = int(line_arr[1])
                user_time = int(line_arr[2])
                stat = float(line_arr[3])
                pe = int(line_arr[4])
                user_event_id = int(line_arr[5])
                
                details = sts_reader.get_user_stat(user_event_id)
                
                data['Function Name'].append('User Stat')
                data['Event Type'].append('User Stat')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)

        
        log_file.close()
        df = pandas.DataFrame(data)
        return df


            







    


reader = ProjectionsReader('../tests/data/ping-pong-projections/pingpong')
trace = reader.read_projections()
print(trace.events)