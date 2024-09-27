from abc import ABC, abstractmethod
from typing import List, Dict
from ..graph import Graph, Node
import numpy


class BaseTraceReader(ABC):


    # The following methods should be called by each reader class
    def create_empty_trace(self, num_processes: int, create_cct: bool) -> None:
        # keep track if we want to create a CCT
        self.create_cct = create_cct

        # keep track of a unique id for each event
        self.unique_id = -1

        # events are indexed by process number, then thread number
        self.events: List[Dict[List[Dict]]] = [{}] * num_processes

        # stacks are indexed by process number, then thread number
        self.stacks: List[Dict[List[int]]] = [{}] * num_processes

        self.ccts: List[Dict[Graph]] = [{}] * num_processes



    def add_event(self, event: Dict) -> None:

        # get process number -- if not present, set to 0
        if "process" in event:
            process = event["process"]
        else:
            process = 0

        # get process number -- if not present, set to 0
        if "thread" in event:
            thread = event["thread"]
        else:
            thread = 0

        # assign a unique id to the event
        event["id"] = self.__get_unique_id()


        # get event list
        if thread not in self.events[process]:
            self.events[process][thread] = []
        event_list: List[Dict] = self.events[process][thread]

        # get stack
        if thread not in self.stacks[process]:
            self.stacks[process][thread] = []
        stack: List[int] = self.stacks[process][thread]

        # if the event is an enter event, add the event to the stack and CCT
        if event["Event Type"] == "Enter":
            cct = None
            # if we are creating a CCT, get the correct CCT
            if self.create_cct:
                if thread not in self.ccts[process]:
                    self.ccts[process][thread] = Graph()
                cct = self.ccts[process][thread]
            self.__update_cct_and_parent_child_relationships(event, self.stacks[process][thread], event_list, cct)
        elif event["Event Type"] == "Leave":
            self.__update_match_event(event, self.stacks[process][thread], event_list)


    def finalize(self) -> None:
        pass

    # Helper methods

    # This method can be thought of the update upon an "Enter" event
    # It adds to the stack and CCT
    def __update_cct_and_parent_child_relationships(self, event: Dict, stack: List[int], event_list: List[Dict], cct: Graph) -> None:
        if len(stack) == 0:
            # root event
            event["parent"] = numpy.nan
            if self.create_cct:
                new_graph_node = Node(event["id"], None)
                cct.add_root(new_graph_node)
                event["Node"] = new_graph_node
        else:
            parent_event = event_list[stack[-1]]
            event["parent"] = parent_event["id"]
            if self.create_cct:
                new_graph_node = Node(event["id"], parent_event["Node"])
                parent_event["Node"].add_child(new_graph_node)
                event["Node"] = new_graph_node

        # update stack and event list
        stack.append(len(event_list) - 1)
        # event_list.append(event)

    
    # def __update_cct(self, event: Dict, process: int, thread: int) -> None:
    #     pass

    # This method can be thought of the update upon a "Leave" event
    # It pops from the stack and updates the event list
    # We should look into using this function to add artificial "Leave" events for unmatched "Enter" events
    def __update_match_event(self, leave_event: Dict, stack: List[int], event_list: List[Dict]) -> None:

        while len(stack) > 0:
            enter_event = event_list[stack[-1]]

            if enter_event["Name"] == leave_event["Name"]:
                # matching event found

                # update matching event ids
                leave_event["_matching_event"] = enter_event["id"]
                enter_event["_matching_event"] = leave_event["id"]

                # popping matched events from the stack
                stack.pop()
                break
            else:
                # popping unmatched events from the stack
                stack.pop()

        # event_list.append(leave_event)

    def __get_unique_id(self) -> int:
        self.unique_id += 1
        return self.unique_id