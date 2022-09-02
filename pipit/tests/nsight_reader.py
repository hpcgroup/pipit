# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from pipit.readers.nsight_reader import NSightReader
import pandas as pd
from pandas.testing import assert_frame_equal

dirname = "data/nbody-nvtx/test_reader.csv"

cols = ["Name", "RangeStack", "Event Type", "Time"]

data = pd.read_csv(dirname)


# Check dataframe is loaded correctly
def test_dataframe():
    fun = NSightReader(dirname)
    assert_frame_equal(fun.df, data)


# Checking the Reader

# Check if the reader does the correct work
def test_len_reader():
    fun = NSightReader(dirname)
    assert len(fun.read().events) == len(data) * 2
    assert len(fun.read().events.columns.values) == len(data.columns.values)


# Test if the nsight reader made changes to the columns
def test_reader_columns():
    fun = NSightReader(dirname)
    assert cols == list(fun.read().events.columns.values)


# Test if the column changes happened or not
def test_column_changes():
    fun = NSightReader(dirname)
    assert fun.df.columns.values is not fun.read().events.columns.values


# Test of the dataframe is ascending by time
def test_ascending_by_time():
    fun = NSightReader(dirname)
    assert fun.read().events["Time"].is_monotonic_increasing


# Test if the reader output is the same as given
def test_reader_output():
    fun = NSightReader(dirname)
    compare = [
        ["main", ":1", "Enter", 0],
        ["bar", ":1:2", "Enter", 5],
        ["bar", ":1:2", "Exit", 1351],
        ["main", ":1", "Exit", 12934],
    ]
    df = pd.DataFrame(compare, columns=["Name", "RangeStack", "Event Type", "Time"])
    assert_frame_equal(df, fun.read().events)


# Checking the Call Graph

# Check if create graph works correctly
def test_graph():
    fun = NSightReader(dirname)

    output = "\t" * 0 + "main\n" + "\t" * 1 + "bar\n"

    assert fun.call_graph().get_graphs() == output
