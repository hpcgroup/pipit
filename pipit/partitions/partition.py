from debugpy.launcher.debuggee import process

from .event import Event
from typing import Set, List, Dict

class Partition:
    # Each Partition is started as a singular event consisting of an MPI operation
    # We will later add in computation events  
    
    def create_empty_partition(self, partition_id: int):
        """
        Constructor for empty partition 
        """
        self.partition_id: int = partition_id
        
        # event's set 
        # each id in events correlates to the trace.events df
        self.events: Set[Event] = set()
        
        # for all p in parents, p happens before self
        self.parents: Set[Partition] = set()
        # for all c in children, c happens after self
        self.children: Set[Partition] = set()

        self.processes: Set[int] = set()
        

        # variables for leap
        self.distance = 0
        self.min_event_start: float = float('inf')
        self.max_event_end: 0
        # self.__calc_min_max_time()

        # Variables for Tarjan's algorithm
        self._initialize_for_tarjan()

    def __init__(self, event: Event):
        # if event is an int, then we are creating an empty partition
        if isinstance(event, int):
            self.create_empty_partition(event)
            return
        self.partition_id: int = event.event_id
        
        # event's set 
        # each id in events correlates to the trace.events df
        self.events: Set[Event] = set()
        self.events.add(event)
        
        # for all p in parents, p happens before self
        self.parents: Set[Partition] = set()
        # for all c in children, c happens after self
        self.children: Set[Partition] = set()

        self.processes: Set[int] = set()
        self.processes.add(event.process)

        # variables for leap
        self.distance = 0
        self.min_event_start: float = event.start_time
        self.max_event_end: float = event.end_time

        # Variables for Tarjan's algorithm
        self._initialize_for_tarjan()

    def __hash__(self) -> int:
        return self.partition_id

    def __eq__(self, other):
        return self.partition_id == other.partition_id

    def __ne__(self, other):
        return self.partition_id != other.partition_id

    def absorb_partition(self, other: "Partition"):
        # absorb events
        self.events.update(other.events)
        # absorb parents
        self.parents.update(other.parents)
        # absorb children
        self.children.update(other.children)
        # absorb processes
        self.processes.update(other.processes)
        # remove self reference from parents and children
        if self in self.parents:
            self.parents.remove(self)
        if self in self.children:
            self.children.remove(self)
        # remove other from parents and children
        if other in self.parents:
            self.parents.remove(other)
        if other in self.children:
            self.children.remove(other)
        # update links in parents
        for parent in other.parents:
            parent.children.remove(other)
            parent.children.add(self)
        # update links in children
        for child in other.children:
            child.parents.remove(other)
            child.parents.add(self)
        # update min_event_start and max_event_end
        if other.min_event_start < self.min_event_start:
            self.min_event_start = other.min_event_start
        if other.max_event_end > self.max_event_end:
            self.max_event_end = other.max_event_end

    def add_processes(self, processes: Set[int]):
        self.processes.update(processes)

    def _initialize_for_tarjan(self):
        self.visited = False
        self.index = -1
        self.low_link = -1

    def add_event(self, e : Event):
        self.event_dict[e.event_id] = e
        e.add_partition(self)
        self.events.add(e)
        self.processes.add(e.process)

        if e.start_time < self.min_event_start:
            self.min_event_start = e.start_time

        if e.end_time > self.max_event_end:
            self.max_event_end = e.end_time

    def add_child(self, child: "Partition"):
        self.children.add(child)
        child.parents.add(self)
