# Copyright 2017-2021 Lawrence Livermore National Security, LLC and otherfrom xml.etree.ElementTree import ElementTree
from asyncio.format_helpers import _get_function_source
from xml.etree.ElementTree import Element, ElementTree
import pandas as pd

from Graph import Graph, Node



class ExperimentReader:
	
	
	def __init__(self, file_location):
		self.tree = ElementTree(file = file_location)

	def get_function_name(self, metric_id):

		#first get id in procedure table
		search = './/S[@it=\"' + str(metric_id) + '\"]..'
		e = self.tree.find(search)
		procedure_table_id = e.get('n')
		return self.get_function_name_helper(procedure_table_id)

		

	def get_function_name_helper(self, procedure_table_id):
		#return function name
		procedure_table_search = './/Procedure[@i=\'' + procedure_table_id + '\']'
		procedure = self.tree.find(procedure_table_search)
		return procedure.get('n')

	def get_min_max_time(self):
		search = './/TraceDB[@i=\"0\"]'
		e = self.tree.find(search)
		time = (int(e.get('db-min-time')), int(e.get('db-max-time')))
		return time

	def create_graph(self):
		graph = Graph()
		call_path_data = list(list(self.tree.getroot())[-1])[-1]
		root_elems = list(call_path_data)
		for root_elem in root_elems:
			# procedure_table_id = root_elem.attrib['n']
			# function_name = self.get_function_name_helper(procedure_table_id)
			# node = Node(procedure_table_id, function_name, None)
			node = self.graph_helper(None, root_elem, graph)
			if node != None:
				graph.add_root(node)	

		return graph

	def graph_helper(self, parent_node: Node, curr_element: Element, graph: Graph):
		if(curr_element.tag == 'PF'):
			procedure_table_id = curr_element.attrib['n']
			function_name = self.get_function_name_helper(procedure_table_id)
			new_node = Node(procedure_table_id, function_name, parent_node)
			if(parent_node != None):
				parent_node.add_child(new_node)

			for child_elem in list(curr_element):
				self.graph_helper(new_node, child_elem, graph)
			return new_node
		
		else:
			calling_context_id = curr_element.attrib.get('it')
			if(calling_context_id != None):
				parent_node.add_calling_contex_id(calling_context_id)
				graph.add_to_map(calling_context_id, parent_node)

			for child_elem in list(curr_element):
				self.graph_helper(parent_node, child_elem, graph)
			return None
			


		

		




	
		


class ProfileReader:


	def __init__(self, file_location):
		self.file = open(file_location, "rb")
		file = self.file
		file.seek(32)
		
		# need to test to see if correct
		byte_order = 'big'
		signed = False



		# Profile Info section offset (pi_ptr)
		
		self.pi_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

	def read_info(self, prof_info_idx):
		byte_order = 'big'
		signed = False
		file = self.file
		

		# Profile Info
		file.seek(self.pi_ptr + (prof_info_idx * 52))
		idt_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
		
		#skipping because not in use 
		# file.read(24) 

		# num_vals = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
		# num_nzctxs = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
		# prof_off = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)


		# Hierarchical Identifier Tuple
		file.seek(idt_ptr)
		num_tuples = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
		# print('num_tuples', num_tuples)
		tuples_list = []
		for i in range(0, num_tuples, 1):
			# not working --  I don't know why, but the second 2 tuples are just incorrect
			kind = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
			p_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
			l_val = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
			tuples_list.append((kind, p_val, l_val))
		return tuples_list

		

def read_header(dir_location):

	# open file
	file = open(dir_location + "trace.db", "rb")

	experiment_reader = ExperimentReader(dir_location + "experiment.xml")

	profile_reader = ProfileReader(dir_location + "profile.db")

	# create graph
	graph = experiment_reader.create_graph()
	
	# read Magic identifier ("HPCPROF-tracedb_")
	encoding = 'ASCII' #idk just guessing rn
	identifier = str(file.read(16), encoding)
	
	# read version
	version_major = file.read(1)
	version_minor = file.read(1)

	# need to test to see if correct
	byte_order = 'big'
	signed = False

	# Number of trace lines (num_traces)
	num_traces = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
	# print("num_traces", num_traces)

	# Number of sections (num_sec)
	num_sections = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)
	# print("num_sections", num_sections)

	# Trace Header section size (hdr_size)
	hdr_size = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
	
	# Trace Header section offset (hdr_ptr)
	hdr_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
	# print("hdr_size: ", hdr_size)

	data = {'Function Name':[], 'Enter Time':[], 'Exit Time':[], 'ID':[], 'Process':[], 'Graph_Node': [] }
	min_max_time = experiment_reader.get_min_max_time()
	trace_df = pd.DataFrame(columns=['Function Name', 'Time', 'Process', 'Graph Node', 'Level'])


	# cycle through trace headers/lines
	for i in range(0, hdr_size, 22):
		proc_num = int(i/22)
		file.seek(hdr_ptr + i)
		# prof_info_idx (in profile.db)
		prof_info_idx = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
		

		# Trace type
		trace_type = int.from_bytes(file.read(2), byteorder=byte_order, signed=signed)

		# Offset of Trace Line start (line_ptr) 
		line_ptr = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)
		
		# Offset of Trace Line one-after-end (line_end) 
		line_end = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed)

		last_id = -1

		last_node = None


		for j in range (line_ptr, line_end, 12):
			file.seek(j)
		
			# Timestamp (nanoseconds since epoch)
			timestamp = int.from_bytes(file.read(8), byteorder=byte_order, signed=signed) - min_max_time[0]
			
			# Sample calling context id (in experiment.xml)
			# can use this to get name of function from experiement.xml Procedure tab
			calling_context_id = int.from_bytes(file.read(4), byteorder=byte_order, signed=signed)
			function_name = experiment_reader.get_function_name(calling_context_id)

			if(last_id != calling_context_id):
				

				# updating the primitive dataframe
				if(last_id != -1):
					# exited the last function
					data['Exit Time'].append(timestamp)
				
				#entered the new function
				data['Enter Time'].append(timestamp)
				data['Function Name'].append(function_name)
				data['ID'].append(calling_context_id)
				data['Process'].append(proc_num)
				data['Graph_Node'].append(graph.get_node(calling_context_id))





				# updating the trace_db

				node = graph.get_node(calling_context_id)
				
				# closing functions exited
				close_node = last_node
				intersect_level = -1
				intersect_node = node.get_intersection(last_node)
				if(intersect_node != None):
					intersect_level = intersect_node.get_level()	
				while close_node != None and close_node.get_level() > intersect_level:
					trace_df = trace_df.append({'Function Name': close_node.name, 'Time': ('Exit', timestamp), 'Process': proc_num, 'Graph Node': close_node, 'Level': close_node.get_level()}, ignore_index=True)
					close_node = close_node.parent
				

				
				# creating new rows for the new functions entered 
				enter_list = node.get_node_list(intersect_level)
				for enter_node in enter_list[::-1]:
					trace_df = trace_df.append({'Function Name': enter_node.name, 'Time': ('Enter', timestamp), 'Process': proc_num, 'Graph Node': enter_node, 'Level': enter_node.get_level()}, ignore_index=True)
				last_node = node
					
			last_id = calling_context_id

		# adding last data for primitive df
		data['Exit Time'].append(min_max_time[1] - min_max_time[0])


		# adding last data for trace df
		close_node = last_node
		while close_node != None:
			trace_df = trace_df.append({'Function Name': close_node.name, 'Time': ('Exit', min_max_time[1] - min_max_time[0]), 'Process': proc_num, 'Graph Node': close_node, 'Level': close_node.get_level()}, ignore_index=True)
			close_node = close_node.parent

	# primitive_df doesn't have full trace, but has just the top level function at all times
	primitive_df = pd.DataFrame(data) 

	return (trace_df, primitive_df)

		

		
file_loc = "../../../data/ping-pong-database-smaller/" # HPCToolKit database location 
trace_data = read_header(file_loc)
print(trace_data[0].to_string())
print(trace_data[1].to_string())


# er = ExperimentReader(file_loc + "experiment.xml")
# er.create_graph()

		
	

