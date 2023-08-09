import random
import textwrap
import pandas as pd
import numpy as np


class FakeNode:
    """
    A single node of the calling tree used to produce fake traces.
    Represents a single function call.
    """

    def __init__(
        self, name, exc_time, mpi_type="", mpi_tgt=0, mpi_volume=0, mpi_time=0
    ):
        self.name = name
        self.exc_time = exc_time
        self.inc_time = exc_time
        self.children = {}  # run_time -> child node
        self.is_mpi = mpi_type != ""
        self.mpi_type = mpi_type
        self.mpi_tgt = mpi_tgt
        self.mpi_volume = mpi_volume
        self.mpi_time = mpi_time
        self.total_nodes = 1
        self.parent = None

    def grow_inc_time(self, time):
        """
        This function adjusts inclusive time metric when a new child is added,
        adding the time to each parent's inc_time, up to the root.
        """
        self.inc_time += time
        if self.parent is not None:
            self.parent.grow_inc_time(time)

    def grow_total_nodes(self, amt):
        """
        This function adjusts the total count of nodes when a new node is added,
        adding the new count to each parent's total_nodes, up to the root.
        """
        self.total_nodes += amt
        if self.parent is not None:
            self.parent.grow_total_nodes(amt)

    def add_child(self, child, run_time):
        """
        This function adds a child that executes after run_time
        exclusive time within the function represented by the current node.
        """
        assert run_time not in self.children
        self.children[run_time] = child
        child.parent = self
        self.grow_total_nodes(child.total_nodes)
        self.grow_inc_time(child.inc_time)

    def choose_random_node(self):
        """
        This function selects a random node, with all descendants
        of the current node being weighted equally, and returns it.
        """
        if not self.children:
            return self
        rng = random.random()
        total = 0
        for child in self.children.values():
            weight = child.total_nodes / self.total_nodes
            if rng < weight:
                return child.choose_random_node()
            else:
                rng -= weight
        return self

    def pick_by_name(self, name):
        """
        Returns all nodes in this tree that have the given name.
        """
        valid = [self] if self.name == name else []
        for child in self.children.values():
            valid += child.pick_by_name(name)
        return valid

    def __str__(self) -> str:
        return "{} ({})\n".format(self.name, self.exc_time) + "\n".join(
            [
                textwrap.indent(str(run_time) + ": " + str(child), "\t")
                for run_time, child in sorted(self.children.items())
            ]
        )

    def mpi_attributes(self):
        """
        Returns the Attributes dictionary for this node, including
        (if it is an MPI event) receiver/sender and msg_length.
        """
        if not self.is_mpi:
            return {}
        attr = {"msg_length": self.mpi_volume}
        if self.mpi_type == "MpiSend":
            attr["receiver"] = self.mpi_tgt
        else:
            attr["sender"] = self.mpi_tgt
        return attr

    def to_events(self, begin_time, process, data):
        """
        Returns event data for this tree, with time starting at begin_time.
        data is an array that is built up and then converted to a DataFrame
        once the entire tree has been processed.
        """
        data.append(
            [
                begin_time,
                "Enter",
                self.name + "()",
                process,
                {},
                self.inc_time,
                self.exc_time,
            ]
        )
        if self.is_mpi:
            data.append(
                [
                    begin_time + self.mpi_time,
                    "Instant",
                    self.mpi_type,
                    process,
                    self.mpi_attributes(),
                    float("nan"),
                    float("nan"),
                ]
            )
        # total_time accumulates durations of already processed children
        total_time = begin_time
        for run_time, child in sorted(self.children.items()):
            # children will add their own lines to data
            child.to_events(total_time + run_time, process, data)
            total_time += child.inc_time

        # time.inc and time.exc are both NaN for Leave events
        data.append(
            [
                begin_time + self.inc_time,
                "Leave",
                self.name + "()",
                process,
                {},
                float("nan"),
                float("nan"),
            ]
        )

    def tweak_tree(self):
        """
        Adds small exclusive time perturbations to
        function length to generate a "similar" tree, for other processes
        to use, that is not identical to the original.
        """
        exc_time = self.exc_time
        factor = (0.7) + random.random() * 0.6
        exc_time_new = exc_time * factor
        # preserve inclusive time relations
        self.grow_inc_time(exc_time_new - exc_time)
        self.exc_time = exc_time_new
        for run_time, child in self.children.items():
            child.tweak_tree()
        # also scale back child run times
        self.children = {
            run_time * factor: child for run_time, child in self.children.items()
        }

    def deepcopy(self):
        """
        Returns a deep copy of the tree.
        """
        mycopy = FakeNode(
            self.name,
            self.exc_time,
            self.mpi_type,
            self.mpi_tgt,
            self.mpi_volume,
            self.mpi_time,
        )
        mycopy.inc_time = self.inc_time
        mycopy.total_nodes = self.total_nodes
        for run_time, child in self.children.items():
            mycopy.children[run_time] = child.deepcopy()
            mycopy.children[run_time].parent = mycopy
        return mycopy

    def node_at_time(self, target_time):
        """
        Returns the node that contains the given target_time,
        as well as the offset into that node that the time occurs.
        """
        total_time = 0
        # TODO: may be more efficient to store self.children sorted already
        for run_time, child in sorted(self.children.items()):
            if target_time < total_time + run_time:
                # this time occurs in the current node!
                return self, target_time - total_time
            elif target_time < total_time + run_time + child.inc_time:
                # this time occurs in the given child node
                return child.node_at_time(target_time - total_time - run_time)
            else:
                # this time occurs after this child
                total_time += child.inc_time
        # if no children find it, it must be the current node
        return self, target_time - total_time

    def insert_at_time(self, child, target_time):
        """
        Adds the child to the proper node such that it executes
        at target_time.
        """
        node, offset = self.node_at_time(target_time)
        node.add_child(child, offset)


def gen_fake_node(function_names):
    """
    Generates a node with a random numeric name and execution time.
    """
    return FakeNode(random.choice(function_names), random.random() * 10)


def gen_fake_tree(num_nodes, function_names, copy_subtrees=True):
    """
    Generates a whole tree of FakeNodes by randomly appending children.
    """
    root = gen_fake_node(function_names)
    # continue to add nodes until we've reached the target
    while root.total_nodes < num_nodes:
        node = gen_fake_node(function_names)
        # choose a node that's currently in the graph to add child to
        parent = root.choose_random_node()
        # select a random point for that child to run
        run_time = random.random() * parent.exc_time
        # find nodes with the same name to copy off of
        same_name = root.pick_by_name(node.name)
        if not same_name or not copy_subtrees:
            parent.add_child(node, run_time)
        else:
            subtree = random.choice(same_name)
            # larger subtrees are less likely to be copied
            if random.random() > 4 / (subtree.total_nodes**0.5):
                parent.add_child(node, run_time)
            else:
                subtree = subtree.deepcopy()
                subtree.tweak_tree()
                parent.add_child(subtree, run_time)

    return root


def gen_forest(seed_tree, num_trees):
    """
    Generates num_trees new trees by tweaking seed_tree.
    """
    forest = [seed_tree.deepcopy() for n in range(num_trees)]
    for tree in forest:
        tree.tweak_tree()
    return forest


def add_fake_mpi_events(trees, num_pairs):
    """
    Adds fake MPIevents to a set of trees (one per process). In total,
    num_pairs pairs of Send/Recv events are generated and inserted.
    Each event is a function with is_mpi=True.
    """
    planned_evts = []
    # choose times for events to happen
    last_proc = -1
    maxtime = min([t.inc_time for t in trees])
    for i in range(2 * num_pairs):
        planned_evts.append(random.random() * maxtime)
    # sort from last to first events in timeline
    # iterate from first to last to avoid
    # dependencies among the events' times
    planned_evts.sort(reverse=True)
    while planned_evts:
        # pair two first events
        first_evt = planned_evts.pop()
        second_evt = planned_evts.pop()
        # time that the first one has to idle
        idle_time = second_evt - first_evt
        # pick two different processes
        first_proc, second_proc = random.sample(range(len(trees)), 2)
        first_tree = trees[first_proc]
        second_tree = trees[second_proc]
        # either first process sends (1) or receives (0)
        send_first = random.randint(0, 1)
        volume = random.randint(1, 1000000)
        # give both a small, random, extra time ("latency", etc)
        if send_first:
            # mpi sends don't need to block
            idle_time = 0
        first_dur = random.random() + idle_time
        second_dur = random.random()
        first_node = FakeNode(
            "MPI_Send" if send_first else "MPI_Recv",
            first_dur,
            "MpiSend" if send_first else "MpiRecv",
            second_proc,
            volume,
            random.random() * first_dur,
        )
        second_node = FakeNode(
            "MPI_Recv" if send_first else "MPI_Send",
            second_dur,
            "MpiRecv" if send_first else "MpiSend",
            first_proc,
            volume,
            random.random() * second_dur,
        )
        first_tree.insert_at_time(first_node, first_evt)
        second_tree.insert_at_time(second_node, second_evt)


def emit_tree_data(trees):
    """
    Writes trees (one per process) as a CSV and returns them.
    At the same time, return ground truth function call information.
    The ground truth data will contain columns corresponding to Pipit's
    time.inc, time.exc.
    """
    data = []
    for process, tree in enumerate(trees):
        # add small random fudge factor, so that we don't have many times of exactly 0
        # which would lead to undefined sorting order and rows not matching
        tree.to_events(random.random() * 0.01, process, data)

    dataframe = pd.DataFrame(
        data,
        None,
        [
            "Timestamp (s)",
            "Event Type",
            "Name",
            "Process",
            "Attributes",
            "time.inc",
            "time.exc",
        ],
    ).sort_values("Timestamp (s)")
    data_csv = dataframe[
        ["Timestamp (s)", "Event Type", "Name", "Process", "Attributes"]
    ].to_csv(index=False)
    ground_csv = dataframe[["time.inc", "time.exc"]].to_csv(index=False)
    return data_csv, ground_csv


def generate_trace(
    num_events,
    num_processes,
    function_names=["foo", "bar", "baz", "quux", "grault", "garply", "waldo"],
    num_mpi_pairs=0,
):
    """
    Top level test generation function. Generates test and ground truth datasets with a
    minimum of num_events Enter/Leave events per process, of which there are
    num_processes. Optionally, MPI events can be added.
    """
    seed_tree = gen_fake_tree(num_events // 2, function_names)
    forest = gen_forest(seed_tree, num_processes)
    add_fake_mpi_events(forest, num_mpi_pairs)
    return emit_tree_data(forest)
