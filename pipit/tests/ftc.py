# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pipit.ftc as ftc
import pandas as pd

func = [
    "foo",
    "bar",
    "baz",
    "grault",
    "qux",
    "waldo",
    "bozo",
    "gerald",
    "boo",
    "new",
    "sheesh",
    "box",
    "tomato",
    "apple",
    "grapes",
    "orange",
]

ftc.FakeCreator(func).create_trace()

func.append("main")

dirname = "data/nbody-nvtx/fake_trace.csv"

df = pd.read_csv(dirname)

print(df)


# Check if function names are within list
def test_function_names_valid():

    # Functions in the dataframe are in the function list given
    name = df["Name"].values.tolist()
    assert all(x in func for x in name)

    # Extra function in dataframe.
    name.append("pain")
    assert not (all(x in func for x in name))


# Check if column names are "Name, Start (ns), End (ns), RangeStack"
def test_column_names():
    cols = ["Name", "Start (ns)", "End (ns)", "RangeStack"]

    assert cols == list(df.columns.values)


# Check if parent function is in the list of children functions
def test_parent_children():
    parent_name = None
    for i in range(1, len(df)):
        if df.iloc[i]["RangeStack"].count(":") == 2:
            parent_name = df.iloc[i]["Name"]
        else:
            assert df.iloc[i]["Name"] != parent_name


# Check function start and end time don't overlap
def test_start_end():
    for index, row in df.iterrows():
        assert row["Start (ns)"] < row["End (ns)"]


# Check if rangestack is correct
def test_range_stack():
    # Check if main fucntion has a rangestakc :1
    if df.iloc[0]["RangeStack"] != ":1":
        assert False

    # Update range_stack
    r_stack = ":1"
    stack = []

    stack.append(df.iloc[0])

    # Going through the dataframe to compare the range stack
    for i in range(1, len(df)):
        curr_start = df.iloc[i]["Start (ns)"]
        prev_end = stack[len(stack) - 1]["End (ns)"]

        # If parent function has a child function. Add an extra :# to the range stack
        # Example:      Name    Start   End     Range Stack
        #               Foo     500     720     1:2
        #               Boo     550     660     1:2:3       <- Updating this Range Stack

        if prev_end > curr_start:
            stack.append(df.iloc[i])
            r_stack = r_stack + ":" + str(int(r_stack.split(":")[-1]) + 1)

        # If the curr function has no children function.
        else:
            count = 0
            # Pop out the current functions.
            # Update the range stack by removing the :# from the end
            # Example:      Name    Start   End     Range Stack
            #               Foo     500     720     1:2
            #               Boo     550     660     1:2:3
            #               Bar     730     770     1:4      <- Updating this rangestack
            #                                             from Boo's rangestack to Bar's

            while stack[len(stack) - 1]["End (ns)"] < curr_start:
                stack.pop()

                # Removing currently running function
                count += 1

                # Updating Parent function from children. See Example above
                if count == 2:
                    r_new = str(int(r_stack.split(":")[-1]) + 1)
                    num = len(r_stack.split(":")[-1]) + 1 + len(r_stack.split(":")[-2])
                    r_stack = r_stack[:-num] + r_new
                    assert r_stack == df.iloc[i]["RangeStack"]

            # Updating Children range stack by increasing by 1 at the end of Range Stack
            # Example:      Name    Start   End     Range Stack
            #               Foo     500     800     1:2
            #               Boo     550     660     1:2:3
            #               Bar     700     770     1:2:4       <- Updating the range
            #                                                   stack from 1:2:3 (Boo)
            #                                                   to 1:2:4 (Bar)
            stack.append(df.iloc[i])
            if count != 2:
                r_stack = r_stack[: -(len(r_stack.split(":")[-1]))] + str(
                    int(r_stack.split(":")[-1]) + 1
                )
                assert r_stack == df.iloc[i]["RangeStack"]


# Check if times overlap
def test_time_overlap():
    stack = []

    stack.append(df.iloc[0])

    # For loop goes through checking times
    for i in range(1, len(df)):
        # Grabbing current function start time and end time of previous function
        curr_start = df.iloc[i]["Start (ns)"]
        prev_end = stack[len(stack) - 1]["End (ns)"]

        # Case #1 Parent Function has Children Function(s)
        # Check if Current function is inside the previous function
        #
        # Example:      Name    Start   End     Range Stack
        #               Foo     500     720     1:2
        #               Boo     480     660     1:2:3       Boo start time is before
        #                                                   it's Parent Functions
        #                                                   start time

        if prev_end > curr_start and stack[len(stack) - 1]["Start (ns)"] < curr_start:
            stack.append(df.iloc[i])

        # Case #2 Current Function is outside the Previous
        # function aka not a Child Function
        elif prev_end < curr_start:

            # Check if End Time of Children Function are
            # greater than Parent Function End Time
            #
            # Example:      Name    Start   End     Range Stack
            #               Foo     500     720     1:2
            #               Boo     550     760     1:2:3       Boo is a child function
            #                                                   of Foo, but ends after
            #                                                   foo does

            if stack[len(stack) - 1]["End (ns)"] < df.iloc[i]["End (ns)"] and stack[
                len(stack) - 1
            ]["RangeStack"].count(":") < df.iloc[i]["RangeStack"].count(":"):
                assert False

            # Remove previous functions from the stack
            while stack[len(stack) - 1]["End (ns)"] < curr_start:
                stack.pop()

            # Check if End Time of previous Parent function is greater than
            # Start time of current Parent Function
            #
            # Example:      Name    Start   End     Range Stack
            #               Foo     500     720     1:2         End time is greater than
            #               Boo     550     660     1:2:3       Bar's start time
            #               Bar     700     770     1:4

            # Check if Start Time of current Parent Function is less than End Time of
            # previous Parent Function
            #
            # Example:      Name    Start   End     Range Stack
            #               Foo     500     700     1:2
            #               Boo     550     660     1:2:3
            #               Bar     680     770     1:4         Start time is less than
            #                                                   Foo's end time

            if stack[len(stack) - 1]["End (ns)"] > curr_start and stack[len(stack) - 1][
                "RangeStack"
            ].count(":") == df.iloc[i]["RangeStack"].count(":"):
                assert False

            stack.append(df.iloc[i])

        else:
            assert False

    assert True


test_function_names_valid()
test_column_names()
test_time_overlap()
test_start_end()
test_parent_children()
test_range_stack()
