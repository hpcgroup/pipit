# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pipit.trace
import json


class PytorchReader:
    """Reader for PyTorch trace files"""

    def __init__(self, file_name) -> None:
        self.file_name = file_name
        self.df = None

    def read(self):
        
        with(open(self.file_name, 'r')) as file:
            data = json.load(file)
            trace_events = data["traceEvents"]
            print(len(trace_events))
            tids = set(map(lambda x: x['tid'], trace_events))
            pids = set(map(lambda x: x['pid'], trace_events))
            #print(tids)
            #print(pids)

            #f = list(filter(lambda x: x['pid'] == '', trace_events))
            #print(f)

            # Get all X events
            x_events = list(filter(lambda x: x['ph'] == 'X', trace_events))

            print(type(x_events[0]['ts']))
            print(type(x_events[0]['dur']))

            # Convert events into list
            x_mapped = list(map(lambda x: ([x['ts'], 'Enter', x['name'], x['pid']], [x['ts'] + x['dur'], 'Leave', x['name'], x['pid']]), x_events))

            temp = [item for sublist in x_mapped for item in sublist]
            x_df = pd.DataFrame(temp, columns=['Timestamp (ns)', 'Event Type', 'Name', 'Process'])

            print(x_df.head())


if __name__ == "__main__":
    filename = "../../docs/examples/UNet_3.5B_4GPUs.json"
    reader = PytorchReader(filename)
    reader.read()
