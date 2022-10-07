import readline

from numpy import NaN
import projections_constants
import gzip
import pandas
import pipit.trace

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
            

class LogReader:
    def __init__(self, executable_location) -> None:
        self.executable_location = executable_location
        self.sts_reader = STSReader(self.executable_location + '.prj.sts')

    
    def read_file(self, pe_num):
        log_file = gzip.open(self.executable_location + '.prj.' + str(pe_num) + '.log.gz', 'rt')
        data = {
            "Function Name": [],
            "Event Type": [],
            "Time": [],
            "Process": [],
            'Details': [],
            'Created By': []
        }
        # Basing read on projections log reader and log entry viewer
        for line in log_file:
            line_arr = line.split()

            if not line_arr[0].isnumeric():
                pass
            
            elif int(line_arr[0]) == projections_constants.BEGIN_IDLE:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Idle')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')

                
            elif int(line_arr[0]) == projections_constants.END_IDLE:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Idle')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')

                
            elif int(line_arr[0]) == projections_constants.BEGIN_PACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Pack')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_PACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Pack')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.BEGIN_UNPACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Unpack')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_UNPACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])

                data['Function Name'].append('Unpack')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.USER_SUPPLIED:
                user_supplied = line_arr[1]
                
                data['Function Name'].append('User Supplied')
                data['Event Type'].append('User Supplied')
                data['Time'].append(-1)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(user_supplied)

            elif int(line_arr[0]) == projections_constants.USER_SUPPLIED_NOTE:
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
                
            elif int(line_arr[0]) == projections_constants.USER_SUPPLIED_BRACKETED_NOTE:
                time = line_arr[1]
                end_time = line_arr[2]
                user_event_id = line_arr[3]
                note = 'Event Name: "' + self.sts_reader.get_event_name(user_event_id) + '" Note: "'
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
                
                
            elif int(line_arr[0]) == projections_constants.MEMORY_USAGE:
                memory_usage = int(line_arr[1])
                time = int(line_arr[2])

                data['Function Name'].append('Memory Usage')
                data['Event Type'].append('Memory Usage')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append(memory_usage)
                
            elif int(line_arr[0]) == projections_constants.CREATION:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                send_time = int(line_arr[7])
                self.sts_reader.get_entry_name(entry)

                data['Function Name'].append('Create')
                data['Event Type'].append('Create')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(self.sts_reader.get_entry_name(entry))
                
            elif int(line_arr[0]) == projections_constants.CREATION_MULTICAST:
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

                
            elif int(line_arr[0]) == projections_constants.BEGIN_PROCESSING:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                recv_time = int(line_arr[7])
                dimensions = self.sts_reader.get_dimension(entry)
                id = []
                for i in range(8, 8 + dimensions):
                    id.append(int(line_arr[i]))
                cpu_start_time = int(line_arr[8 + dimensions])

                num_perf_counts = self.sts_reader.get_num_perf_counts()
                perf_counts = []
                for i in range(9 + dimensions, 9 + dimensions + num_perf_counts):
                    perf_counts.append(int(line_arr[i]))
                
                data['Function Name'].append('Processing')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(self.sts_reader.get_entry_name(entry))
                
            elif int(line_arr[0]) == projections_constants.END_PROCESSING:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                cpu_end_time = int(line_arr[7])
                num_perf_counts = self.sts_reader.get_num_perf_counts()
                perf_counts = []
                for i in range(8 + dimensions, 8 + dimensions + num_perf_counts):
                    perf_counts.append(int(line_arr[i]))
                
                data['Function Name'].append('Processing')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(self.sts_reader.get_entry_name(entry))
                
            elif int(line_arr[0]) == projections_constants.BEGIN_TRACE:
                time = int(line_arr[1])

                data['Function Name'].append('Begin Trace')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')

            elif int(line_arr[0]) == projections_constants.END_TRACE:
                time = int(line_arr[1])

                data['Function Name'].append('Begin Trace')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.MESSAGE_RECV:
                mtype = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                message_length = int(line_arr[5])
                
            elif int(line_arr[0]) == projections_constants.ENQUEUE:
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

            elif int(line_arr[0]) == projections_constants.DEQUEUE:
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
                
            elif int(line_arr[0]) == projections_constants.BEGIN_INTERRUPT:
                time = int(line_arr[1])
                event = int(line_arr[2])
                pe = int(line_arr[3])

                data['Function Name'].append('Interrupt')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_INTERRUPT:
                time = int(line_arr[1])
                event = int(line_arr[2])
                pe = int(line_arr[3])

                data['Function Name'].append('Interrupt')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.BEGIN_COMPUTATION:
                time = int(line_arr[1])
                
                data['Function Name'].append('Computation')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_COMPUTATION:
                time = int(line_arr[1])
                
                data['Function Name'].append('Computation')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(-1)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.USER_EVENT:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])

                details = self.sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event')
                data['Event Type'].append('User Event')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == projections_constants.USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = self.sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event Pair')
                data['Event Type'].append('User Event Pair')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == projections_constants.BEGIN_USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = self.sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event Pair')
                data['Event Type'].append('Entry')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == projections_constants.END_USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = self.sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event Pair')
                data['Event Type'].append('Exit')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == projections_constants.USER_STAT:
                time = int(line_arr[1])
                user_time = int(line_arr[2])
                stat = float(line_arr[3])
                pe = int(line_arr[4])
                user_event_id = int(line_arr[5])
                
                details = self.sts_reader.get_user_stat(user_event_id)
                
                data['Function Name'].append('User Stat')
                data['Event Type'].append('User Stat')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Created By'].append(pe)
                data['Details'].append(details)

        
        log_file.close()
        return data


            







    


# reader = LogReader('../tests/data/ping-pong-projections/pingpong')
# trace = reader.read_file(0)
# print(trace)

