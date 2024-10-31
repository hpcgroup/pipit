import pandas as pd
from typing import Set, List, Dict
# import networkx as nx
from . import Partition, Event
from .. import Trace
from graphviz import Digraph

class Partition_DAG:
    """
    Class to house Partition DAG for Lateness Calculation
    """
    # class to house Partition DAG
    # Not yet concrete, but need to start somewhere
    # Creation of the partition dag will happen in two phases
    # Phase 1.
    #   Adding in singleton partitions with the only connections being happens before/after in the same process
    # Phase 2:
    #  Adding in the connections between partitions that are not in the same process (Communication)

    # Phase 1
    def __init__(self, root_partitions: [], all_processes: Set[int]) -> None:
        self.roots: Set[Partition] = set(root_partitions)
        self.df = pd.DataFrame(columns=['Partition ID', 'Distance'])
        
        self.leaves: Set[Partition] = set(root_partitions)
        
        self.partition_map: Dict[int, Partition] = {}
        for p in root_partitions:
            self.partition_map[p.partition_id] = p
        
        self.all_processes = all_processes
        self.global_step_df = None
    
    def add_partition(self, partition: Partition, happens_after: Partition) -> None:
        # adds in the happens after relationship between partitions
        partition.parents.add(happens_after)
        happens_after.children.add(partition)
        # keep track of leaves
        if happens_after in self.leaves:
            self.leaves.remove(happens_after)
        self.leaves.add(partition)
        self.partition_map[partition.partition_id] = partition

    # Phase 2
    def add_connection(self, from_id: int, to_id: int) -> None:
        # adds in the communication connection between partitions
        from_partition = self.partition_map[from_id]
        to_partition = self.partition_map[to_id]
        from_partition.add_child(to_partition)
        # keep track of leaves
        if from_partition in self.leaves:
            self.leaves.remove(from_partition)
    
    # All together DAG creation
    @staticmethod
    def create_dag_from_Trace(trace: Trace) -> "Partition_DAG":
        # we need events and messages matched to create the DAG
        trace._match_events()
        trace._match_messages()

        events_df = trace.events

        # Set Up Root Partition
        all_processes = set(events_df['Process'].unique())
        root_partition = Partition(-1)
        root_partition.add_processes(all_processes)

        # Create Initial DAG
        dag = Partition_DAG([root_partition], all_processes)

        # Filter DF for Communication Enter Events
        send_wrapper_event_names = ["MPI_Send"]
        send_instant_event_names = ["MpiSend"]
        recv_wrapper_event_names = ["MPI_Recv"]

        events_df = events_df.loc[events_df['Name'].isin(send_wrapper_event_names + recv_wrapper_event_names)]
        events_df = events_df.loc[events_df['Event Type'] == 'Enter'].sort_values(by=['Timestamp (ns)'])

        # Get the columns index for access later
        time_col_index = events_df.columns.get_loc('Timestamp (ns)') + 1
        matching_time_col_index = events_df.columns.get_loc('_matching_timestamp') + 1
        message_matching_event_col_index = events_df.columns.get_loc('_matching_message_event') + 1

        # Add all the communication events as partitions to the dag
        # First iterate through all the processes
        for process in all_processes:

            process_events: pd.DataFrame = events_df.loc[events_df['Process'] == process]
            prev_partition = root_partition

            for row in process_events.itertuples():
                # Currently in an Enter Event for a communication event
                # The matching message event is the index of the instant communication event
                # The instant communication event provides information on the matching event (Send/Recv)
                # The Enter Event provides information on the matching time (Enter/Leave)
                # Since the Instant Event contains information about matching send/receive,
                # we can use that as the event/partition id.

                # Instant Comm Event index in the trace.events df
                event_id = int(row[message_matching_event_col_index])

                # time of the Enter Event
                start_time = row[time_col_index]
                # time of Leave Event
                end_time = row[matching_time_col_index]

                # Create new Event for partition
                event = Event(event_id, process, start_time, end_time)
                # Create Singleton Partition
                partition = Partition(event)
                partition.add_processes({process})
                # Add Partition to DAG
                dag.add_partition(partition, prev_partition)
                prev_partition = partition
        
        # Merge all the communication connections
        events_df = trace.events
        # Filter DF for Send Instant Events
        events_df = events_df.loc[events_df['Event Type'] == 'Instant']
        comm_df = events_df.loc[events_df['Name'].isin(send_instant_event_names)]
        for row in comm_df.itertuples():
            send_event = row.Index
            recv_event = row[message_matching_event_col_index]
            # dag.add_connection(send_event, recv_event)
            # Merge Send and Recv Partitions
            dag.merge_partitions(send_event, recv_event)
        # After Merging the dag is not guaranteed to be a DAG
        # so we need to merge SCCs

        return dag

    def merge_partitions(self, partition_id_1: int, partition_id_2: int) -> None:
        # merges two partitions
        partition_1 = self.partition_map[partition_id_1]
        partition_2 = self.partition_map[partition_id_2]
        partition_1.absorb_partition(partition_2)
        self.partition_map.pop(partition_id_2)
        
        # update the leaves
        if partition_2 in self.leaves:
            self.leaves.remove(partition_2)
        if len(partition_1.children) == 0 and partition_1 not in self.leaves:
            self.leaves.add(partition_1)

    def get_dot_representation(self) -> Digraph:
        # returns a dot representation of the partition DAG
        dot = Digraph()
        for partition in self.partition_map.values():
            dot.node(str(partition.partition_id))
            for child in partition.children:
                dot.edge(str(partition.partition_id), str(child.partition_id))
        return dot
