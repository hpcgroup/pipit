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
        self.visited = False
        self.index = -1
        self.low_link = -1
    
    


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
        

        # variables for leap
        self.distance = 0
        self.min_event_start: float = float('inf')
        self.max_event_end: float = 0
        self.__calc_min_max_time()

        # Variables for Tarjan's algorithm
        self.visited = False
        self.index = -1
        self.low_link = -1
    

    def __hash__(self) -> int:
        return self.partition_id

    def __eq__(self, other):
        return self.partition_id == other.partition_id

    def __ne__(self, other):
        return self.partition_id != other.partition_id

    def absorb_partition(self, other: "Partition"):
        self.events.update(other.events)
        
        self.parents.update(other.parents)
        self.children.update(other.children)
        self.processes.update(other.processes)

        if self in self.parents:
            self.parents.remove(self)
        if self in self.children:
            self.children.remove(self)
        if other in self.parents:
            self.parents.remove(other)
        if other in self.children:
            self.children.remove(other)
        
        return self

    def add_processes(self, processes: Set[int]):
        self.processes.update(processes)

    def __calc_min_max_time(self):
        if len(self.events) > 0:    
            self.min_event_start = min([event.start_time for event in self.events])
            self.max_event_end = max([event.end_time for event in self.events])

    def initialize_for_tarjan(self):
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

    @staticmethod
    def tarjan_strongly_connected(graph):
        """ Tarjan's Algorithm for finding strongly connected components
        Parameters
        ----------
        graph : dict
            Dictionary of partition_id -> Partition
        Returns
        -------
        components : list
            List of strongly connected components
        """
        index = 0
        stack = []
        components = []
        visited = set()

        def strong_connect(partition):
            nonlocal index, stack, components, visited

            partition.index = index
            partition.low_link = index
            index += 1

            stack.append(partition)
            visited.add(partition.partition_id)

            for child_id in partition.get_children():
                if child_id not in visited:
                    child = graph[child_id]
                    strong_connect(child)
                    partition.low_link = min(partition.low_link, child.low_link)
                elif graph[child_id] in stack:
                    partition.low_link = min(partition.low_link, graph[child_id].index)

            if partition.low_link == partition.index:
                component = []
                while stack:
                    top = stack.pop()
                    component.append(top.partition_id)
                    if top == partition:
                        break
                components.append(component)

        for _,partition in graph.items():
            partition.initialize_for_tarjan()

        for partition_id, partition in graph.items():
            if partition_id not in visited:
                strong_connect(partition)

        return components

    @staticmethod
    def merge_strongly_connected_components(graph, components):
        """ Merge strongly connected components into one partition
        Parameters
        ----------
        graph : dict
            Dictionary of partition_id -> Partition
        components : list
            List of strongly connected components
        Returns
        -------
        merged_graph : dict
            Dictionary of partition_id -> Partition
        """
        # Note that the original graph is also modified since merge_partition is inplace so one 
        # should not use the original graph after calling this function
        merged_graph = {}
        for component in components:
            merged_graph[component[0]] = graph[component[0]]
            if len(component) > 1:
                for partition_id in component[1:]:
                    merged_graph[component[0]].merge_partition(graph[partition_id])
        return merged_graph