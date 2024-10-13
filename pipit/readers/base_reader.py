from abc import ABC, abstractmethod
from typing import List, Dict

import pandas
import pandas as pd

from .. import Trace
from ..graph import Graph, Node
import numpy


class BaseTraceReader(ABC):

    @abstractmethod
    def read(self) -> Trace:
        pass


    # The following methods should be called by each reader class
    def create_empty_trace(self, num_processes: int) -> None:
        # keep track of a unique id for each event
        self.unique_id = -1

        # events are indexed by process number, then thread number
        # stores a list of events
        self.events: List[Dict[int, List[Dict]]] = []
        for i in range(num_processes):
            self.events.append({})

        # stacks are indexed by process number, then thread number
        # stores indices of events in the event list
        self.stacks: List[Dict[int, List[int]]] = [{}] * num_processes



    def add_event(self, event: Dict) -> None:

        # get process number -- if not present, set to 0
        if "Process" in event:
            process = event["Process"]
        else:
            print("something is wrong")
            process = 0

        # get thread number -- if not present, set to 0
        if "Thread" in event:
            print("something is wrong")
            thread = event["Thread"]
        else:
            thread = 0
            # event["Thread"] = 0

        # assign a unique id to the event
        event["unique_id"] = self.__get_unique_id()


        process_events = self.events[process]
        process_stacks = self.stacks[process]

        # get event list
        if thread not in process_events:
            process_events[thread] = []
        event_list = process_events[thread]

        # get stack
        if thread not in process_stacks:
            process_stacks[thread] = []
        stack: List[int] = process_stacks[thread]

        # if the event is an enter event, add the event to the stack and update the parent-child relationships
        if event["Event Type"] == "Enter":
            self.__update_parent_child_relationships(event, stack, event_list)
        # if the event is a leave event, update the matching event and pop from the stack
        elif event["Event Type"] == "Leave":
            self.__update_match_event(event, stack, event_list)

        event_list.append(event)
        x = 0

    def finalize_process(self, process: int) -> pd.DataFrame:
        # first step put everything in one list
        # all_events = []
        # for process in self.events:
        #     for thread in process:
        #         all_events.extend(process[thread])

        # convert 3d list of events to 1d list
        all_events = []
        for thread_id in self.events[process]:
            all_events.extend(self.events[process][thread_id])
                # df = pd.DataFrame(self.events[proc_id][thread_id])
                # just_for_break = 0

                # for i in range(len(self.events[proc_id][thread_id])):
                #     all_events.append(self.events[proc_id][thread_id][i])
                #     pass
                    # print(self.events[i][j][k]['Process'])

        # print('all_events has length: ' + str(len(all_events)))
        # for i in range(len(self.events)):
        #     print(f'self.events[{i}] has length', len(self.events[i].keys()))
        #     # print(type(self.events[i]))
        #     # print (self.events[i].keys())
        #     for j in self.events[i]:
        #         # print(j)
        #         # print (j in self.events[i])
        #         # print(self.events[i][j])
        #         print(f'self.events[{i}][{j}] has length', len(self.events[i][j]))

        # df_list = []
        # for process in self.events:
        #     for thread in process:
        #         df_list.append(pd.DataFrame(process[thread]))
        # all_events = pd.concat(df_list)

        # create a dataframe
        df = pd.DataFrame(all_events)
        # print(df.head())
        # print(self.events_dataframe["unique_id"].value_counts())
        return df
        # self.events_dataframe = pandas.DataFrame(all_events)
        # print number of events per id
        # print(self.events_dataframe.sort_values(by=["unique_id"]))
        # self.trace =  Trace(None, self.events_dataframe, None)

    # Helper methods

    # This method can be thought of the update upon an "Enter" event
    # It adds to the stack and CCT
    def __update_parent_child_relationships(self, event: Dict, stack: List[int], event_list: List[Dict]) -> None:
        if len(stack) == 0:
            # root event
            event["parent"] = numpy.nan
        else:
            parent_event = event_list[stack[-1]]
            event["parent"] = parent_event["unique_id"]


        # update stack
        stack.append(len(event_list))


    # This method can be thought of the update upon a "Leave" event
    # It pops from the stack and updates the event list
    # We should look into using this function to add artificial "Leave" events for unmatched "Enter" events
    def __update_match_event(self, leave_event: Dict, stack: List[int], event_list: List[Dict]) -> None:

        while len(stack) > 0:

            # popping matched events from the stack
            enter_event = event_list[stack.pop(-1)]


            if enter_event["Name"] == leave_event["Name"]:
                # matching event found

                # update matching event ids
                leave_event["_matching_event"] = enter_event["unique_id"]
                enter_event["_matching_event"] = leave_event["unique_id"]

                break


    def __get_unique_id(self) -> int:
        self.unique_id += 1
        return self.unique_id