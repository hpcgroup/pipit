from xml.etree.ElementTree import ElementTree

class Node:
	
	def __init__(self, name_id, name, parent) -> None:
		self.calling_context_ids = []
		self.name_id = name_id
		self.name = name
		self.children = []
		self.parent = parent
	def add_child(self, child_node):
		self.children.append(child_node)
	def add_calling_contex_id(self, calling_context_id):
		self.calling_context_ids.append(calling_context_id)

	def __repr__(self) -> str:
		return self.name + ": " + str(self.calling_context_ids)

	def __str__(self) -> str:
		return self.name + ": " + str(self.calling_context_ids)



class Graph:

	def __init__(self) -> None:
		self.roots = []
		self.calling_context_id_map = {}

	def add_to_map(self, calling_context_id, node):
		self.calling_context_id_map[calling_context_id] = node

	def add_root(self, node):
		self.roots.append(node)
	
	def get_node(self, calling_context_id):
		return self.calling_context_id_map.get(str(calling_context_id))
		

