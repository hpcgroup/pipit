# from .event import Event
# from .partition_old import Partition
# from pandas import DataFrame

# # def _get_event_list(trace):
# #     event_list = []
# #     unique_processes = trace.events['Process'].unique().tolist()
# #     unique_processes.sort()
# #     for process in unique_processes:
# #         process_filtered_events: DataFrame = trace.events[(trace.events['Process'] == process)]

# #         prev_event = None
# #         prev_event_row = None
# #         for index, event_row in process_filtered_events.iterrows():
# #             event = None
# #             event_name = event_row['Name']
# #             event_type = event_row['Event Type']

# #             if event_type == 'Instant':
# #                 if event_name


# def _get_event_graph(trace):
#     """ Get event graph from trace
#     Parameters
#     ----------
#     trace : pipit.Trace
#         Trace object
    
#     Returns
#     -------
#         start_event_list : list
#             List of start events
#         event_dict : dict
#             Dictionary of all events with event id as key and event object as value
#     """
#     unique_processes = trace.events['Process'].unique().tolist()
#     unique_processes.sort()


#     # List of start events - one for each process
#     start_event_ids = []

#     # Dictionary of all events with event id as key and event object as value
#     # Does not include the start event and event id is the index of the event in the trace
#     event_dict = {}

#     for process in unique_processes:
#         process_filtered_events = trace.events[(trace.events['Process'] == process)]

#         prev_event = None
#         prev_event_row = None
#         for index, event_row in process_filtered_events.iterrows():
#             event = None

#             if event_row['Event Type'] == 'Instant':
#                 # The first event for every process should be ProgramBegin and this will be our start event
#                 if event_row['Name'] == 'ProgramBegin' or event_row['Name'] == 'ProgramEnd':
#                     event = Event(event_row['Name'], event_row['Event Type'], index, event_row['Timestamp (ns)'], process)
#                     if event_row['Name'] == 'ProgramBegin':
#                         start_event_ids.append(index)
#                 elif event_row['Name'] == 'MpiSend' or event_row['Name'] == 'MpiRecv':
#                     # Only for MPI events, we care about the matching event for now
#                     # Assumption - For a particular (process, thread) MPI Enter, Instant and Leave will be in that order and that order only
#                     event = Event(event_row['Name'], event_row['Event Type'], index, event_row['Timestamp (ns)'], process, event_row['_matching_event'])
#                     event.add_start_time(prev_event_row['Timestamp (ns)'])
#                 else:
#                     print (f"Skipped event - {event_row['Name']}")

#                 if event is not None:
#                     if prev_event is not None:
#                         event.add_prev_event(prev_event)
#                         prev_event.add_next_event(event)

#                     event_dict[index] = event
#                     prev_event = event

#             elif event_row['Event Type'] == 'Enter':
#                 pass
#             elif event_row['Event Type'] == 'Leave':
#                 # For Leave events, we need to add the end time to the prev event
#                 if event_row['Name'] == 'MPI_Send' or event_row['Name'] == 'MPI_Recv':
#                     prev_event.add_end_time(event_row['Timestamp (ns)'])

#             prev_event_row = event_row

#     # Iteratate through all events and store the object of the matching event in the current event
#     for start_event_id in start_event_ids:
#         event = event_dict[start_event_id]
#         while event is not None:
#             if event.matching_event_id != -1:
#                 event.add_matching_event(event_dict[event.matching_event_id])
#             event = event.get_next_event()

#     return (start_event_ids, event_dict)


# def get_partition_graph(trace):
#     """ Get Initial Partition graph from trace
#     Parameters
#     ----------
#     trace : pipit.Trace
#         Trace object
    
#     Returns
#     -------
#     start_partition_ids : list
#         List of start partition ids
#     partition_dict : list
#         Dictionary of partition id -> Partition
#     """

#     start_event_ids, event_dict = _get_event_graph(trace)

#     partition_dict = {} # partition_id -> Partition
#     start_partition_ids = []

#     event_to_partition = {} # event_id -> partition_id

#     # Create a singleton partition for every event
#     for start_event in start_event_ids:
#         event = event_dict[start_event]
#         while event is not None:
#             if event.has_matching_event() and (event.matching_event_id in event_to_partition.keys()):
#                 # If the matching event is already in a partition, add the current event to that partition
#                 partition_dict[event_to_partition[event.matching_event_id]].add_event(event) # Also sets the partition to the event
#                 event_to_partition[event.event_id] = event_to_partition[event.matching_event_id]
#             else:
#                 partition_id = len(partition_dict.keys())
#                 partition_dict[partition_id] = Partition(partition_id, [event])
#                 event_to_partition[event.event_id] = partition_id

#             if event.event_name == "ProgramBegin":
#                 start_partition_ids.append(event_to_partition[event.event_id])

#             event = event.next_event

#     # TODO: Maybe, I need to return the event_list 
#     return (start_partition_ids, partition_dict)

