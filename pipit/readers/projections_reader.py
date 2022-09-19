import readline
import projections_constants

class STSReader:
    def __init__(self, file_location):
        self.sts_file = open(file_location, 'r')
        self.chares = {}
        self.entry_chares = {}
        self.messages = []
        self.read_file()

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
                
        print(self.chares)
        print('\n\n\n')
        print(self.entry_chares)





    


x = STSReader('../tests/data/ping-pong-projections/pingpong.prj.sts')

