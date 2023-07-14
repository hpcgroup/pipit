import random
import textwrap
import pandas as pd


class FakeNode:
    """
    A single node of the calling tree used to produce fake traces.
    Represents a single function call.
    """

    def __init__(self, name, exc_time):
        self.name = name
        self.exc_time = exc_time
        self.children = {}  # run_time -> child node

    def add_child(self, child, run_time):
        """
        This function adds a child that executes after run_time
        exclusive time within the function represented by the current node.
        """
        self.children[run_time] = child

    def calc_inc_time(self):
        """
        Similar to the calc_*_metrics functions in trace.py,
        computes inclusive execution time for this tree.
        """
        self.inc_time = self.exc_time
        for run_time, child in self.children.items():
            child.calc_inc_time()
            self.inc_time += child.inc_time

    def __str__(self) -> str:
        return "{} ({})\n".format(self.name, self.exc_time) + "\n".join(
            [
                textwrap.indent(str(run_time) + ": " + str(child), "\t")
                for run_time, child in sorted(self.children.items())
            ]
        )

    def to_events(self, begin_time, process, data):
        """
        Returns event data for this tree, with time starting at begin_time.
        Inclusive time must have already been computed.
        data is an array that is built up and then converted to a DataFrame
        once the entire tree has been processed.
        """
        data.append(
            [
                begin_time,
                "Enter",
                self.name + "()",
                process,
                self.inc_time,
                self.exc_time,
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
                float("nan"),
                float("nan"),
            ]
        )


def gen_fake_node():
    """
    Generates a node with a random numeric name and execution time.
    """
    return FakeNode("func_" + str(random.randint(0, 1000000)), random.random() * 10)


def gen_fake_tree(num_nodes):
    """
    Generates a whole tree of FakeNodes by randomly appending children.
    """
    nodes = [gen_fake_node() for n in range(num_nodes)]
    root = nodes[0]
    for index, node in enumerate(nodes[1:]):
        # choose a node that's currently in the graph to add child to
        parent = random.choice(nodes[: index + 1])
        # select a random point for that child to run
        run_time = random.random() * parent.exc_time
        parent.add_child(node, run_time)
    return root


def emit_tree_file(trees, test_file, ground_truth_file):
    """
    Writes trees (one per process) as a CSV to the File object test_file.
    At the same time, write ground truth function call information
    to the File object ground_truth_file.
    ground_truth_file will contain columns corresponding to Pipit's
    time.inc, time.exc.
    """
    data = []
    for process, tree in enumerate(trees):
        tree.calc_inc_time()
        # add small random fudge factor, so that we don't have many times of exactly 0
        # which would lead to undefined sorting order and rows not matching
        tree.to_events(random.random(), process, data)

    dataframe = pd.DataFrame(
        data,
        None,
        ["Timestamp (s)", "Event Type", "Name", "Process", "time.inc", "time.exc"],
    ).sort_values("Timestamp (s)")
    dataframe[["Timestamp (s)", "Event Type", "Name", "Process"]].to_csv(
        test_file, index=False
    )
    dataframe[["time.inc", "time.exc"]].to_csv(ground_truth_file, index=False)
