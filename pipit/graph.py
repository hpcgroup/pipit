# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT


class Node:
    """Each Node corresponds to a PF tag in the experiment.xml file, and can be
    referenced by any calling_context_id directly under it
    """

    def __init__(self, id, parent, level=None) -> None:
        self._pipit_nid = id
        self.children = []
        self.parent = parent

        if level is None:
            self.level = self._calculate_level()
        else:
            self.level = level

    def add_child(self, child_node):
        self.children.append(child_node)

    def get_level(self):
        """This function returns the depth of the current node
        (a root node would return 0)
        """
        return self.level

    def get_intersection(self, node: "Node"):
        """Given two nodes, this function returns the interesection of them
        starting from their root nodes (least common ancestor)
        If the two nodes do not share the same root node, their intersection
        would be None, otherwise it returns the nodes that they have in
        common (starting from the root) as a new Node
        """
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
        """creates list from current node to node with level min_level
        backtracks on the current Node until root or min_level (whichever
        comes first) and returns them as a list of Nodes
        """
        node = self
        return_list = []

        while node is not None and node.level > min_level:
            return_list.append(node)
            node = node.parent

        return return_list

    def __str__(self) -> str:
        return "ID: " + str(self._pipit_nid) + " -- Level: " + str(self.level)

    def _calculate_level(self):
        """private function to get depth of node"""
        if self.parent is None:
            return 0
        else:
            return 1 + self.parent._calculate_level()

    def __eq__(self, obj) -> bool:
        if isinstance(obj, Node):
            return self._pipit_nid == obj._pipit_nid
        else:
            return False


class Graph:
    """Represents the calling context tree / call graph"""

    def __init__(self) -> None:
        self.roots = []

    def add_root(self, node):
        self.roots.append(node)

    def __str__(self) -> str:
        return "Roots: " + str([str(curr_root) for curr_root in self.roots])
