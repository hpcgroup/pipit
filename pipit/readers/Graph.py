class Node:
    def __init__(self, name_id, name, parent) -> None:
        self.calling_context_ids = []
        self.name_id = name_id
        self.name = name
        self.children = []
        self.parent = parent
        self.level = self.__calculate_level()

    def add_child(self, child_node):
        self.children.append(child_node)

    def add_calling_contex_id(self, calling_context_id):
        self.calling_context_ids.append(calling_context_id)

    def get_level(self):
        return self.level

    def get_intersection(self, node: "Node"):
        if node is None:
            return None
        if self.get_level() > node.get_level():
            node1 = self
            node2 = node
        else:
            node1 = node
            node2 = self
        while node1.get_level() > node2.get_level():
            node1 = node1.parent
        while node1 != node2:
            node1 = node1.parent
            node2 = node2.parent
        return node1

    def get_node_list(self, min_level):
        # creates list from current node to node with level min_level
        node = self
        return_list = []
        while node is not None and node.level > min_level:
            return_list.append(node)
            node = node.parent
        return return_list

    def __str__(self) -> str:
        return (
            self.name
            + ": "
            + str(self.calling_context_ids)
            + " -- level: "
            + str(self.level)
        )

    # private function to get depth of node
    def __calculate_level(self):
        if self.parent is None:
            return 0
        else:
            return 1 + self.parent.__calculate_level()

    def __eq__(self, obj) -> bool:
        if type(obj) != Node:
            return False
        else:
            return self.calling_context_ids == obj.calling_context_ids


class Graph:
    def __init__(self) -> None:
        self.roots = []
        self.calling_context_id_map = {}

    def add_to_map(self, calling_context_id, node):
        self.calling_context_id_map[calling_context_id] = node

    def add_root(self, node):
        self.roots.append(node)

    def get_node(self, calling_context_id) -> "Node":
        return self.calling_context_id_map.get(str(calling_context_id))
