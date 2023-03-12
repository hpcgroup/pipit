# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest
import numpy as np
import pandas as pd
from pipit import Trace
from pipit.filter import Filter


@pytest.mark.xfail(reason="Allow this to fail until otf2 has a pip package.")
def test_comm_matrix(data_dir, ping_pong_otf2_trace):
    # bytes sent between pairs of processes
    size_comm_matrix = Trace.from_otf2(str(ping_pong_otf2_trace)).comm_matrix()

    # number of messages sent between pairs of processes
    count_comm_matrix = Trace.from_otf2(str(ping_pong_otf2_trace)).comm_matrix("count")

    # 2 ranks in ping pong trace, so comm matrix should have shape 2 x 2
    assert size_comm_matrix.shape == size_comm_matrix.shape == (2, 2)

    # no messages from ranks to themselves
    # note: comm matrix elements accessed using matrix[sender_rank][receiver_rank]
    assert (
        size_comm_matrix[0][0]
        == size_comm_matrix[1][1]
        == count_comm_matrix[0][0]
        == count_comm_matrix[1][1]
        == 0
    )

    # 8 sends from each process (total of 4177920 bytes ~ 3.984 mebibytes)
    assert size_comm_matrix[0][1] == size_comm_matrix[1][0] == 4177920
    assert count_comm_matrix[0][1] == count_comm_matrix[1][0] == 8


@pytest.mark.xfail(reason="Allow this to fail until otf2 has a pip package.")
def test_match_events(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    trace._match_events()

    df = trace.events

    # test both ranks
    rank_0_df = df.loc[(df["Process"] == 0) & (df["Event Type"] != "Instant")]
    rank_1_df = df.loc[(df["Process"] == 1) & (df["Event Type"] != "Instant")]

    # Make lists of normal and matching columns for both indices and
    # timestamps.  Compares the values of these lists to ensure the pairing
    # functions produced correct results.
    rank_0_indices = rank_0_df.index.to_list()
    rank_0_matching_indices = rank_0_df["Matching Index"].to_list()
    rank_0_timestamps = rank_0_df["Timestamp (ns)"].to_list()
    rank_0_matching_timestamps = rank_0_df["Matching Timestamp"].to_list()

    # All events in ping pong trace are at level 0 of the call tree, so the
    # leave row occurs immediately after the enter. The below assertions test
    # this.
    for i in range(len(rank_0_df)):
        if i % 2 == 0:
            # the matching event and timestamp for enter rows
            # should occur right after (ex: (Enter: 45, Leave: 46))
            assert rank_0_matching_indices[i] == rank_0_indices[i + 1]
            assert rank_0_matching_timestamps[i] == rank_0_timestamps[i + 1]
        else:
            # the matching event and timestamp for leave rows
            # should occur right before (ex: (Enter: 45, Leave: 46))
            assert rank_0_matching_indices[i] == rank_0_indices[i - 1]
            assert rank_0_matching_timestamps[i] == rank_0_timestamps[i - 1]

    # tests all the same as mentioned above, except for rank 1 as well
    rank_1_indices = rank_1_df.index.to_list()
    rank_1_matching_indices = rank_1_df["Matching Index"].to_list()
    rank_1_timestamps = rank_1_df["Timestamp (ns)"].to_list()
    rank_1_matching_timestamps = rank_1_df["Matching Timestamp"].to_list()

    for i in range(len(rank_1_df)):
        if i % 2 == 0:
            assert rank_1_matching_indices[i] == rank_1_indices[i + 1]
            assert rank_1_matching_timestamps[i] == rank_1_timestamps[i + 1]
        else:
            assert rank_1_matching_indices[i] == rank_1_indices[i - 1]
            assert rank_1_matching_timestamps[i] == rank_1_timestamps[i - 1]

    # Checks that the Matching Indices and Timestamps for the Enter rows are
    # greater than that of the Leave rows.
    assert (
        np.array(df.loc[df["Event Type"] == "Enter"]["Matching Index"])
        > np.array(df.loc[df["Event Type"] == "Leave"]["Matching Index"])
    ).all()
    assert (
        np.array(df.loc[df["Event Type"] == "Enter"]["Matching Timestamp"])
        > np.array(df.loc[df["Event Type"] == "Leave"]["Matching Timestamp"])
    ).all()


@pytest.mark.xfail(reason="Allow this to fail until otf2 has a pip package.")
def test_match_caller_callee(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    trace._match_caller_callee()

    df = trace.events

    # all events of the ping pong trace are roots with no children
    assert set(df.loc[df["Event Type"] == "Enter"]["Depth"]) == set([0])


@pytest.mark.xfail(reason="Allow this to fail until otf2 has a pip package.")
def test_filter(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    def all_equal(*dfs):
        return all([dfs[0].equals(df) for df in dfs])

    # Basic filter tests
    assert all_equal(
        trace.filter("Process", "==", 0).events,
        trace.filter(Filter("Process", "==", 0)).events,
        trace.filter(Filter(expr="`Process` == 0")).events,
        trace.filter(Filter(func=lambda x: x["Process"] == 0)).events,
        trace.events[trace.events["Process"] == 0],
    )

    assert all_equal(
        trace.filter("Process", "!=", 0).events,
        trace.filter(Filter("Process", "!=", 0)).events,
        trace.filter(Filter(expr="`Process` != 0")).events,
        trace.filter(Filter(func=lambda x: x["Process"] != 0)).events,
        trace.events[trace.events["Process"] != 0],
    )

    assert all_equal(
        trace.filter("Timestamp (ns)", ">", 1.33e8).events,
        trace.filter(Filter("Timestamp (ns)", ">", 1.33e8)).events,
        trace.filter(Filter(expr="`Timestamp (ns)` > 1.33e8")).events,
        trace.filter(Filter(func=lambda x: x["Timestamp (ns)"] > 1.33e8)).events,
        trace.events[trace.events["Timestamp (ns)"] > 1.33e8],
    )

    assert all_equal(
        trace.filter("Timestamp (ns)", "<", 1.33e8).events,
        trace.filter(Filter("Timestamp (ns)", "<", 1.33e8)).events,
        trace.filter(Filter(expr="`Timestamp (ns)` < 1.33e8")).events,
        trace.filter(Filter(func=lambda x: x["Timestamp (ns)"] < 1.33e8)).events,
        trace.events[trace.events["Timestamp (ns)"] < 1.33e8],
    )

    assert all_equal(
        trace.filter("Name", "in", ["MPI_Send", "MPI_Recv"]).events,
        trace.filter(Filter("Name", "in", ["MPI_Send", "MPI_Recv"])).events,
        trace.filter(Filter(expr='`Name`.isin(["MPI_Send", "MPI_Recv"])')).events,
        trace.filter(
            Filter(func=lambda x: x["Name"] in ["MPI_Send", "MPI_Recv"])
        ).events,
        trace.events[trace.events["Name"].isin(["MPI_Send", "MPI_Recv"])],
    )

    assert all_equal(
        trace.filter("Name", "not-in", ["MPI_Send", "MPI_Recv"]).events,
        trace.filter(Filter("Name", "not-in", ["MPI_Send", "MPI_Recv"])).events,
        trace.filter(Filter(expr='~`Name`.isin(["MPI_Send", "MPI_Recv"])')).events,
        trace.filter(
            Filter(func=lambda x: x["Name"] not in ["MPI_Send", "MPI_Recv"])
        ).events,
        trace.events[~trace.events["Name"].isin(["MPI_Send", "MPI_Recv"])],
    )

    assert all_equal(
        trace.filter("Timestamp (ns)", "between", [1.33e8, 1.36e8]).events,
        trace.filter(Filter("Timestamp (ns)", "between", [1.33e8, 1.36e8])).events,
        trace.filter(
            Filter(expr="(`Timestamp (ns)` > 1.33e8) & (`Timestamp (ns)` < 1.36e8)")
        ).events,
        trace.filter(
            Filter(
                func=lambda x: (x["Timestamp (ns)"] > 1.33e8)
                & (x["Timestamp (ns)"] < 1.36e8)
            )
        ).events,
        trace.events[
            (trace.events["Timestamp (ns)"] > 1.33e8)
            & (trace.events["Timestamp (ns)"] < 1.36e8)
        ],
    )

    # Compound filter tests
    f1 = Filter("Timestamp (ns)", "between", [1.33e8, 1.36e8])
    f2 = Filter("Name", "in", ["MPI_Send", "MPI_Recv"])
    f3 = Filter("Process", "==", 0)

    assert all_equal(
        trace.filter(f1 & f2 & f3).events,
        trace.filter(f1).filter(f2).filter(f3).events,
    )

    assert all_equal(
        trace.filter(f1 | f2 | f3).events,
        pd.concat(
            [trace.filter(f1).events, trace.filter(f2).events, trace.filter(f3).events]
        )
        .drop_duplicates(subset=["Timestamp (ns)", "Process", "Thread", "Name"])
        .sort_index(),
    )
