import readline
import projections_constants
import gzip
import pandas
import pipit.trace

class STSReader:
    def __init__(self, file_location):
        self.sts_file = open(file_location, 'r')
        self.chares = {}
        self.entry_chares = {}
        self.messages = []
        self.user_events = {}
        self.user_stats = {}
        self.read_file()

    def get_entry(self, entry_id):
        return self.entry_chares[entry_id]

    def get_user_event(self, user_event_id):
        return self.user_events[user_event_id]

    def get_user_stat(self, user_event_id):
        return self.user_stats[user_event_id]

    def read_file(self):
        for line in self.sts_file:
            line_arr = line.split()
            # print('here')
            if line_arr[0] == 'CHARE':
                id = int(line_arr[1])
                name = line_arr[2][1:len(line_arr[2]) - 1]
                self.chares[id] = name
                # print(int(line_arr[1]), line_arr[2][1:len(line_arr[2]) - 1])
                
            elif line_arr[0] == 'ENTRY':

                # Need to concat entry_name
                while not line_arr[3].endswith('"'):
                    line_arr[3] = line_arr[3] + line_arr[4]
                    del line_arr[4]

                
                id = int(line_arr[2])
                entry_name = line_arr[3][1:len(line_arr[3]) - 1]
                chare_id = int(line_arr[4])
                name = self.chares[chare_id] + '::' + entry_name
                self.entry_chares[id] = name
        

        # self.sts_file.close()
        # print(self.chares)
        # print('\n\n\n')
        # print(self.entry_chares)

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
            'Details': []
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
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')

                
            elif int(line_arr[0]) == projections_constants.END_IDLE:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Idle')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')

                
            elif int(line_arr[0]) == projections_constants.BEGIN_PACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Pack')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_PACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Pack')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.BEGIN_UNPACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])
                data['Function Name'].append('Unpack')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_UNPACK:
                time = int(line_arr[1])
                pe = int(line_arr[2])

                data['Function Name'].append('Unpack')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.USER_SUPPLIED:
                # to implement
                pass
                
            elif int(line_arr[0]) == projections_constants.USER_SUPPLIED_NOTE:
                # to implement
                pass
                
            elif int(line_arr[0]) == projections_constants.USER_SUPPLIED_BRACKETED_NOTE:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.MEMORY_USAGE:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.CREATION:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                msglen = int(line_arr[6])
                send_time = int(line_arr[7])
                self.sts_reader.get_entry(entry)

                data['Function Name'].append('Create')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(self.sts_reader.get_entry(entry))

                data['Function Name'].append('Create')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(self.sts_reader.get_entry(entry))
                
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
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('To ' + str(numPEs) + 'processors')

                data['Function Name'].append('Multicast')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('To ' + str(numPEs) + 'processors')
                
            elif int(line_arr[0]) == projections_constants.BEGIN_PROCESSING:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                # msglen = int(line_arr[6])
                # recv_time = int(line_arr[7])
                # Skipping for now
                # dimensions = get_dimension(entry) # need to implement
                # id = []
                # for i in (0, dimensions):
                #     id.append(int(line_arr[7 + i]))
                # cpu_start_time = int(line_arr[7 + dimensions])
                
                data['Function Name'].append('Processing')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(self.sts_reader.get_entry(entry))
                
            elif int(line_arr[0]) == projections_constants.END_PROCESSING:
                mtype = int(line_arr[1])
                entry = int(line_arr[2])
                time = int(line_arr[3])
                event = int(line_arr[4])
                pe = int(line_arr[5])
                # msglen = int(line_arr[6])
                # recv_time = int(line_arr[7])
                # cpu_end_time
                
                data['Function Name'].append('Processing')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(self.sts_reader.get_entry(entry))
                
            elif int(line_arr[0]) == projections_constants.BEGIN_TRACE:
                # time = int(line_arr[1])
                # not used in log viewer
                pass

            elif int(line_arr[0]) == projections_constants.END_TRACE:
                # time = int(line_arr[1])
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.MESSAGE_RECV:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.ENQUEUE:
                mtype = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                
                data['Function Name'].append('Enque')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('message received from processor ' + pe + ' destined for UNKOWN')
                # data['Details'].append('message received from processor ' + pe + ' destined for ' + self.sts_reader.get_entry())
                data['Function Name'].append('Enque')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('message received from processor ' + pe + ' destined for UNKOWN')
                
            elif int(line_arr[0]) == projections_constants.DEQUEUE:
                # not used in log viewer
                pass
                # mtype = int(line_arr[1])
                # time = int(line_arr[2])
                # event = int(line_arr[3])
                # pe = int(line_arr[4])
                
                # data['Function Name'].append('')
                # data['Event Type'].append('')
                # data['Time'].append(time)
                # data['Process'].append(pe_num)
                # data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.BEGIN_INTERRUPT:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.END_INTERRUPT:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.BEGIN_COMPUTATION:
                time = int(line_arr[1])
                
                data['Function Name'].append('Computation')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.END_COMPUTATION:
                time = int(line_arr[1])
                
                data['Function Name'].append('Computation')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append('')
                
            elif int(line_arr[0]) == projections_constants.USER_EVENT:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])

                details = self.sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('User Event')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(details)

                data['Function Name'].append('User Event')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == projections_constants.USER_EVENT_PAIR:
                user_event_id = int(line_arr[1])
                time = int(line_arr[2])
                event = int(line_arr[3])
                pe = int(line_arr[4])
                nested_id = int(line_arr[5])
                
                details = self.sts_reader.get_user_event(user_event_id)
                
                data['Function Name'].append('Bracketed User Event')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(details)
                
                data['Function Name'].append('Bracketed User Event')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(details)
                
            elif int(line_arr[0]) == projections_constants.BEGIN_USER_EVENT_PAIR:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.END_USER_EVENT_PAIR:
                # not used in log viewer
                pass
                
            elif int(line_arr[0]) == projections_constants.USER_STAT:
                time = int(line_arr[1])
                user_time = int(line_arr[2])
                stat = float(line_arr[3])
                pe = int(line_arr[4])
                user_event_id = int(line_arr[5])
                
                details = self.sts_reader.get_user_stat(user_event_id)
                
                data['Function Name'].append('User Stat')
                data['Event Type'].append('Enter')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(details)

                data['Function Name'].append('User Stat')
                data['Event Type'].append('Leave')
                data['Time'].append(time)
                data['Process'].append(pe_num)
                data['Details'].append(details)

        trace_df = pandas.DataFrame(data)
        return pipit.trace.Trace(None, trace_df)


            







    


reader = LogReader('../tests/data/ping-pong-projections/pingpong')
trace = reader.read_file(0)
print(trace.events)

