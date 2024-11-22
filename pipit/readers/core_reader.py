from typing import List, Dict

import numpy as np
import pandas
import numpy

from pipit.trace import Trace
import narwhals as nw
from narwhals.typing import IntoDataFrameT, FrameT
import narwhals.selectors as ncs

class CoreTraceReader:
    """
    Helper Object to read traces from different sources and convert them into a common
    format
    """

    def __init__(self, start: int = 0, stride: int = 1, frame_backend=pandas.DataFrame):
        """
        Should be called by each process to create an empty trace per process in the
        reader. Creates the following data structures to represent an empty trace:
        - events: Dict[int, Dict[int, List[Dict]]]
        - stacks: Dict[int, Dict[int, List[int]]]
        """
        # keep stride for how much unique id should be incremented
        self.stride = stride

        # keep track of a unique id for each event
        self.unique_id = start - self.stride

        # events are indexed by process number, then thread number
        # stores a list of events
        self.events: Dict[int, Dict[int, List[Dict]]] = {}

        # stacks are indexed by process number, then thread number
        # stores indices of events in the event list
        self.stacks: Dict[int, Dict[int, List[int]]] = {}

        # Set the frame backend
        self.frame_backend = frame_backend

    def add_event(self, event: Dict) -> None:
        """
        Should be called to add each event to the trace. Will update the event lists and
        stacks accordingly.
        """
        # get process number -- if not present, set to 0
        if "Process" in event:
            process = event["Process"]
        else:
            process = 0

        # get thread number -- if not present, set to 0
        if "Thread" in event:
            thread = event["Thread"]
        else:
            thread = 0
            # event["Thread"] = 0

        # assign a unique id to the event
        event["unique_id"] = self.__get_unique_id()

        # get event list
        if process not in self.events:
            self.events[process] = {}
        if thread not in self.events[process]:
            self.events[process][thread] = []
        event_list = self.events[process][thread]

        # get stack
        if process not in self.stacks:
            self.stacks[process] = {}
        if thread not in self.stacks[process]:
            self.stacks[process][thread] = []
        stack: List[int] = self.stacks[process][thread]

        # if the event is an enter event, add the event to the stack and update the
        # parent-child relationships
        if event["Event Type"] == "Enter":
            self.__update_parent_child_relationships(event, stack, event_list, False)
        elif event["Event Type"] == "Instant":
            self.__update_parent_child_relationships(event, stack, event_list, True)
        # if the event is a leave event, update the matching event and pop from the
        # stack
        elif event["Event Type"] == "Leave":
            self.__update_match_event(event, stack, event_list)

        # Finally add the event to the event list
        event_list.append(event)

    def finalize(self) -> IntoDataFrameT :
        """
        Converts the events data structure into a pandas dataframe and returns it
        """
        all_events = []
        for process in self.events:
            for thread in self.events[process]:
                all_events.extend(self.events[process][thread])

        # Create a trace_frame
        trace_frame = nw.from_native(self.frame_backend(all_events))

        # Fill null with -1 (since int32 does not allow nan)
        trace_frame = trace_frame.with_columns([
            nw.col(['_matching_event', '_parent', '_matching_timestamp']).fill_null(-1),
        ])

        # Convert Name, Event Type, and Process to categorical memory savings
        # Convert _matching_event, _parent, and _matching_timestamp to int, since they are indices
        trace_frame = trace_frame.with_columns([
            nw.col(['Name', 'Event Type', 'Process']).cast(nw.dtypes.Categorical),
            nw.col(['_matching_event', '_parent', '_matching_timestamp']).cast(nw.dtypes.Int32),
        ])

        # Return native because multiprocessing fails with narwhal frames
        return trace_frame.to_native()

    def __update_parent_child_relationships(
        self, event: Dict, stack: List[int], event_list: List[Dict], is_instant: bool
    ) -> None:
        """
        This method can be thought of the update upon an "Enter" event. It adds to the
        stack and CCT
        """
        if len(stack) == 0:
            # root event
            event["_parent"] = -1
        else:
            parent_event = event_list[stack[-1]]
            event["_parent"] = parent_event["unique_id"]

        # update stack
        if not is_instant:
            stack.append(len(event_list))

    def __update_match_event(
        self, leave_event: Dict, stack: List[int], event_list: List[Dict]
    ) -> None:
        """
        This method can be thought of the update upon a "Leave" event. It pops from the
        stack and updates the event list. We should look into using this function to add
        artificial "Leave" events for unmatched "Enter" events
        """

        while len(stack) > 0:

            # popping matched events from the stack
            enter_event = event_list[stack.pop()]

            if enter_event["Name"] == leave_event["Name"]:
                # matching event found

                # update matching event ids
                leave_event["_matching_event"] = enter_event["unique_id"]
                enter_event["_matching_event"] = leave_event["unique_id"]

                # update matching timestamps
                leave_event["_matching_timestamp"] = enter_event["Timestamp (ns)"]
                enter_event["_matching_timestamp"] = leave_event["Timestamp (ns)"]

                break

    def __get_unique_id(self) -> int:
        self.unique_id += self.stride
        return self.unique_id

def concat_trace_data(data_list: List[IntoDataFrameT]) -> FrameT:
    """
    Concatenates the data from multiple trace readers into a single trace reader
    """
    # Converting into nw frames
    nw_frames = [nw.from_native(df) for df in data_list]

    # Concatenating into a single frame
    trace_frame: nw.DataFrame = nw.concat(nw_frames)

    # Set index to unique_id
    nw.maybe_set_index(trace_frame, "unique_id")

    # Sort by timestamp and unique_id
    trace_frame.sort("Timestamp (ns)", "unique_id")

    # Return Narwhals frame
    return trace_frame