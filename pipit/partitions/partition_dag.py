import pandas as pd
from typing import Set, List, Dict
import networkx as nx
from . import Partition, leap, Event
from .. import Trace

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
        partition.add_parent(happens_after)
        happens_after.add_child(partition)
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
        to_partition.add_parent(from_partition)
        # keep track of leaves
        if from_partition in self.leaves:
            self.leaves.remove(from_partition)
    
    # All together DAG creation
    def create_dag_from_Trace(trace: Trace) -> "Partition_DAG":
        events_df = trace.events

        # Set Up Root Partition
        all_processes = set(events_df['Process'].unique())
        intial_time = events_df['Time'].min()
        root_partition = Partition(-1)
        root_partition.add_processes(all_processes)

        # Create Initial DAG
        dag = Partition_DAG([root_partition], all_processes)

        # Filter DF for Communication Events
        events_df = events_df.loc[events_df['name'].isin(['MpiSend', 'MpiRecv'])]
        events_df = events_df.loc[events_df['Event Type'] == 'Enter'].sort_values(by=['Time'])

        # Add all the communication events as partitions to the dag
        # TODO: Need to investigate if matching event/timestamp is for leave or respective MPI event
        time_col_index = events_df.columns.get_loc('Timestamp (ns)') + 1
        for process in all_processes:
            process_events: pd.DataFrame = events_df.loc[events_df['Process'] == process]
            
            prev_partition = root_partition
            for row in process_events.itertuples():
                event_id = row.Index
                process = row.Process
                start_time = row[time_col_index]
                end_time = row._matching_timestamp

                event = Event(event_id, process, start_time, end_time)
                partition = Partition(event)
                partition.add_processes({process})
                dag.add_partition(partition, prev_partition)
                prev_event.add_matching_event(event)
                prev_event = event
                prev_partition = partition
        
        # Merge all the communication connections
        comm_df = events_df.loc[events_df['name'] == 'MpiSend']
        for row in comm_df.itertuples():
            send_event = row.Index
            recv_event = row._matching_event
            # dag.add_connection(send_event, recv_event)
            dag.merge_partitions(send_event, recv_event)
        
        return dag

    def merge_partitions(self, partition_id_1: int, partition_id_2: int) -> None:
        # merges two partitions
        partition_1 = self.partition_map[partition_id_1]
        partition_2 = self.partition_map[partition_id_2]
        partition_1.absorb_partition(partition_2)
        self.partition_map.pop(partition_id_2)
        
        # update the leaves
        self.leaves.remove(partition_2)
        if len(partition_1.children) == 0 and partition_1 not in self.leaves:
            self.leaves.add(partition_1)


    # def calculate_distance(self) -> None:
    #     # calculates the distance of each partition to root and updates the df
    #     def calc_distance_helper(node: Partition):
    #         # calculating distance for this node
    #         dist = 0
    #         parent_ids = node.get_parents()
    #         if len(parent_ids) != 0:
    #             for parent_id in parent_ids:
    #                 parent = self.partition_map[parent_id]
    #                 dist = max(parent.distance, dist)
    #             dist += 1
    #             # print(dist)
    #         dist = max(node.distance, dist)
    #         node.distance = dist
    #         # self.df.at[self.df['Partition ID'] == node.partition_id]['Distance'] = dist
    #         index = self.df[self.df['Partition ID'] == node.partition_id].index[0]
    #         self.df.at[index, 'Distance'] = dist

    #         # print("Now calculating distance for child nodes...")

    #         # calculating distance for child nodes
    #         for child_id in node.get_children():
    #             print("processing ", child_id)
    #             child_node = self.partition_map[child_id]
    #             # print('child node', child_node)
    #             calc_distance_helper(child_node)

    #     print("There are ", len(self.roots), " roots in here")

    #     roots = list(self.roots)

    #     for i in range(len(roots)):
    #         print("Root number ", i)
    #         calc_distance_helper(roots[i])


    # def create_leaps(self) -> None:
    #     # creates leap partitions
    #     # assumes that distance has been calculated

    #     self.leaps: List[Leap] = []
    #     max_distance = int(self.df['Distance'].max())
    #     for i in range(max_distance + 1):
    #         partition_ids = self.df[self.df['Distance'] == i]['Partition ID'].values.tolist()
    #         leap = Leap(self.partition_map, self.all_processes, partition_ids)
    #         self.leaps.append(leap)

    # def leap_distance(self, partition: Partition, leap_id: int, incoming: bool) -> float:
    #     # calculates the incoming/outgoing leap distance
    #     # TODO: implement this
    #     # print(leap_id, len(self.leaps))
    #     if leap_id < 0 or leap_id >= len(self.leaps):
    #         return float('inf')
    #     if incoming:
    #         return partition.min_event_start - self.leaps[leap_id].max_event_end
    #     else:
    #         return self.leaps[leap_id].min_event_start - partition.max_event_end

    # def much_smaller(self, incoming: int, outgoing: int) -> bool:
    #     # to calculate incoming << outgoing from the paper's psudo-code
    #     return incoming < (outgoing / 10)

    # def will_expand(self, partition_id: int, leap: Leap) -> bool:
    #     # returns true if partition encompass processes that aren't in the leap
    #     return leap.partition_will_expand(partition_id)

    # def absorb_partition(self, parent: Partition, child_id: int, parent_leap_id: int) -> None:
    #     # child partition is merged into parent partition
    #     # print('absorbing partition', child_id, 'into partition', parent.partition_id)
    #     child = self.partition_map[child_id]

    #     child_parents = child.get_parents()
    #     child_children = child.get_children()
    #     child_leap_id = child.distance

    #     parent.merge_partition(child)

    #     for p in child_parents:
    #         p = self.partition_map[p]
    #         p.get_children()
    #     for c in child_children: 
    #         c = self.partition_map[c]
    #         c.get_parents()

    #     self.leaps[child_leap_id].remove_partition(child.partition_id)
    #     self.leaps[parent_leap_id].calc_processes()
    #     self.partition_map.pop(child.partition_id)


    # def absorb_next_leap(self, leap_id: int) -> None:
    #     # merges leap_id + 1 into leap_id
    #     print('absorbing next leap', leap_id, leap_id + 1, len(self.leaps))
    #     self.leaps[leap_id].absorb_leap(self.leaps[leap_id + 1])
    #     self.leaps.pop(leap_id + 1)

    # def merge_partition_to_leap(self, partition: Partition, leap_to_id: int, leap_from_id: int) -> None:
    #     # merges partition into leap
    #     print('merging partition', partition.partition_id, 'to leap', leap_to_id)
    #     self.leaps[leap_to_id].add_partition(partition.partition_id)
    #     self.leaps[leap_from_id].remove_partition(partition.partition_id)




    # def complete_leaps(self, force_merge: bool):
    #     # the algorithm from the paper to "Complete leaps through merging paritions"
    #     all_leaps = self.leaps
    #     k = 0
    #     while k < len(all_leaps):
    #         leap = all_leaps[k]
    #         changed = True
    #         while changed and not leap.is_complete:
    #             changed = False
    #             as_list = list(leap.partitions_ids)
    #             for partition_id in as_list:
    #                 p = self.partition_map[partition_id]
    #                 incoming = self.leap_distance(p, k - 1, incoming = True)
    #                 outgoing = self.leap_distance(p, k + 1, incoming = False)
    #                 if self.much_smaller(incoming, outgoing):
    #                     self.merge_partition_to_leap(p, k - 1, k)
    #                     changed = True
    #                 else:
    #                     for c in p.children:
    #                         if self.will_expand(c, leap):
    #                             self.absorb_partition(p, c, k)
    #                             changed = True
    #         if not leap.is_complete and force_merge:
    #             self.absorb_next_leap(k)
    #         else:
    #             k = k + 1

    # def global_step_assignment(self):
    #     # the algorithm from the paper to "Assign global steps to events"
    #     next_global_step = 0
    #     # global_step_df = pd.DataFrame(columns=['PartitionId', 'EventId', 'EventName', 'Stride', 'NextStride', 'Step'])
    #     global_step_df = pd.DataFrame()
    #     for leap in self.leaps:
    #         if leap.is_leap_empty():
    #           continue

    #         step_df = leap.calculate_local_step()
    #         step_df, next_global_step = leap.add_global_step(step_df, next_global_step)

    #         global_step_df = pd.concat([global_step_df, step_df])

    #     self.global_step_df = global_step_df.sort_values(by=['Step']).reset_index(drop=True)

    #     matching_step_col = []
    #     matching_process_col = []
    #     for index, row in self.global_step_df.iterrows():
    #         matching_event_id = row['Matching Event ID']
    #         matching_step = None
    #         matching_process = None
    #         if matching_event_id != None:
    #             matching_row = self.global_step_df[self.global_step_df['EventId'] == matching_event_id]
    #             if len(matching_row) > 0:
    #                 matching_step = matching_row.iloc[0]['Step']
    #                 matching_process = matching_row.iloc[0]['Process']
    #         matching_step_col.append(matching_step)
    #         matching_process_col.append(matching_process)
    #     self.global_step_df['Matching Step'] = matching_step_col
    #     self.global_step_df['Matching Process'] = matching_process_col


    # def calculate_lateness(self):
    #     # calculates the lateness of each event
    #     # assumes that global steps have been assigned

    #     self.global_step_df['Lateness'] = None

    #     unique_steps = self.global_step_df['Step'].unique().tolist()
    #     unique_steps.sort()

    #     for step in unique_steps:
    #         step_df = self.global_step_df[self.global_step_df['Step'] == step]

    #         min_exit = float('inf')
    #         for index, row in step_df.iterrows():
    #             event = self.partition_map[row['PartitionId']].event_dict[row['EventId']]

    #             if event.end_time < min_exit:
    #                 min_exit = event.end_time

    #         for index, row in step_df.iterrows():
    #             event = self.partition_map[row['PartitionId']].event_dict[row['EventId']]

    #             event.lateness = event.end_time - min_exit
    #             self.global_step_df.at[index, 'Lateness'] = event.lateness

    # def calculate_differential_lateness(self):
    #     # calculates the differential lateness of each event
    #     # assumes that lateness has been calculated

    #     self.global_step_df['DiffLateness'] = None

    #     for index, row in self.global_step_df.iterrows():
    #         event = self.partition_map[row['PartitionId']].event_dict[row['EventId']]

    #         max_lateness = 0.0

    #         prev_event = event.get_prev_event()
    #         if prev_event is not None and prev_event.lateness > max_lateness:
    #             max_lateness = prev_event.lateness

    #         if event.event_name == "MpiRecv":
    #             prev_matching_event = event.get_matching_event()
    #             if prev_matching_event is not None and prev_matching_event.lateness > max_lateness:
    #                 max_lateness = prev_matching_event.lateness

    #         event.diff_latenss = max(event.lateness - max_lateness, 0.0)

    #         self.global_step_df.at[index, 'DiffLateness'] = event.diff_latenss


