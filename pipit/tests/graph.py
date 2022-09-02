# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pipit.graph as graph
import pandas as pd

call_graph = graph.Graph()

dirname = "data/nbody-nvtx/test_reader.csv"

df = pd.read_csv(dirname)

root = graph.Node(-1, df.iloc[0]["Name"], None)
root.add_calling_contex_id(df.iloc[0]["RangeStack"])
root.add_time(df.iloc[0]["Start (ns)"], df.iloc[0]["End (ns)"])

call_graph.add_root(root)
call_graph.add_to_map(root.calling_context_ids[0], root)

node = graph.Node(-1, df.iloc[1]["Name"], root)
node.add_calling_contex_id(df.iloc[1]["RangeStack"])
node.add_time(df.iloc[1]["Start (ns)"], df.iloc[1]["End (ns)"])

call_graph.add_to_map(node.calling_context_ids[0], node)

# Check Node


# Check name of nodes
def test_node_name():
    assert root.name == "main"
    assert node.name == "bar"


# Check add child function
def test_add_child():
    root.add_child(node)
    assert root.get_child() == [node]
    assert root.children == [node]
    assert node.get_child() == []
    assert node.children == []


# Check add calling context id
def test_calling_context():
    assert root.calling_context_ids == [":1"]
    assert node.calling_context_ids == [":1:2"]


# Check time being added to the nodes
def test_time():
    assert root.time == [0, 12934]
    assert node.time == [5, 1351]


# Check level of nodes
def test_level():
    assert root.get_level() == 0
    assert node.get_level() == 1


# Check Graph

# Test context map from graph with given inputs
def test_context_map():
    assert call_graph.calling_context_id_map == {":1": root, ":1:2": node}


# Test if the call graph is correct with given input
def test_root():
    assert call_graph.roots == [root]


# Test if the the get node function works to retrieve the given input
def test_get_node():
    assert call_graph.get_node(":1") == root
    assert call_graph.get_node(":1:2") == node


# Test if the is empty function works on the call graph
def test_is_empty():
    assert call_graph.is_empty() is False
    assert call_graph.is_empty() is not True


# Test if the graph output is correct
def test_graph():
    output = "\t" * 0 + "main\n" + "\t" * 1 + "bar\n"

    assert call_graph.get_graphs() == output
