from typing import Set

# Class to store individual events and info on their matching events

# Each event will contain an index
class Event:
    # The event_id is the index of the event in the trace.events DF
    # Matching Events is a set of matching communication events
    #   I.e. MPI Send and MPI Recv

    def __init__(self, event_id: int, process: int, start_time: float, end_time: float):
        """
        Constructor for Event
        @param event_id: int - index of event in trace.events DF
        @param process: int - process id (rank)
        @param start_time: float - time of the event's 'enter' or 'start'
        @param end_time: float - time of the event's 'exit' or 'end'
        """
        self.event_id = event_id
        self.matching_events = Set[int]
        # self.partition_id = None
        self.process = process
        self.start_time = start_time
        self.end_time = end_time
    
    def add_matching_event(self, matching_event_id: int):
        self.matching_events.add(matching_event_id)

    # def set_partition(self, partition_id: int):
    #     self.partition_id = partition_id
