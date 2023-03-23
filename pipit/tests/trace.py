# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
from pipit import Trace


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
    rank_0_matching_indices = rank_0_df["_matching_event"].to_list()
    rank_0_timestamps = rank_0_df["Timestamp (ns)"].to_list()
    rank_0_matching_timestamps = rank_0_df["_matching_timestamp"].to_list()

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
    rank_1_matching_indices = rank_1_df["_matching_event"].to_list()
    rank_1_timestamps = rank_1_df["Timestamp (ns)"].to_list()
    rank_1_matching_timestamps = rank_1_df["_matching_timestamp"].to_list()

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
        np.array(df.loc[df["Event Type"] == "Enter"]["_matching_event"])
        > np.array(df.loc[df["Event Type"] == "Leave"]["_matching_event"])
    ).all()
    assert (
        np.array(df.loc[df["Event Type"] == "Enter"]["_matching_timestamp"])
        > np.array(df.loc[df["Event Type"] == "Leave"]["_matching_timestamp"])
    ).all()


def test_match_caller_callee(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    trace._match_caller_callee()

    df = trace.events

    # all events of the ping pong trace are roots with no children
    assert len(df.loc[(df["Event Type"] == "Enter") & (df["_parent"].notnull())]) == 0


def test_filter(data_dir, ping_pong_otf2_trace):
    from pipit.filter import Filter
    import pytest

    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    def all_equal(*dfs):
        return all([dfs[0].equals(df) for df in dfs])

    # Basic filter tests
    # ensure that we get same result despite using different APIs
    assert all_equal(
        trace.filter("Process", "==", 0).events,
        trace.filter(Filter("Process", "==", 0)).events,
        trace.filter(Filter(expr="`Process` == 0")).events,
        trace.events[trace.events["Process"] == 0],
    )

    assert all_equal(
        trace.filter("Process", "!=", 0).events,
        trace.filter(Filter("Process", "!=", 0)).events,
        trace.filter(Filter(expr="`Process` != 0")).events,
        trace.events[trace.events["Process"] != 0],
    )

    assert all_equal(
        trace.filter("Timestamp (ns)", ">", "130.52 ms").events,
        trace.filter("Timestamp (ns)", ">", 1.3052e08).events,
        trace.filter(Filter("Timestamp (ns)", ">", 1.3052e08)).events,
        trace.filter(Filter(expr="`Timestamp (ns)` > 1.3052e+08")).events,
        trace.events[trace.events["Timestamp (ns)"] > 1.3052e08],
    )

    assert all_equal(
        trace.filter("Timestamp (ns)", "<", "135.67 ms").events,
        trace.filter("Timestamp (ns)", "<", 1.3567e08).events,
        trace.filter(Filter("Timestamp (ns)", "<", 1.3567e08)).events,
        trace.filter(Filter(expr="`Timestamp (ns)` < 1.3567e+08")).events,
        trace.events[trace.events["Timestamp (ns)"] < 1.3567e08],
    )

    assert all_equal(
        trace.filter("Name", "in", ["MPI_Send", "MPI_Recv"]).events,
        trace.filter(Filter("Name", "in", ["MPI_Send", "MPI_Recv"])).events,
        trace.filter(Filter(expr='`Name`.isin(["MPI_Send", "MPI_Recv"])')).events,
        trace.events[trace.events["Name"].isin(["MPI_Send", "MPI_Recv"])],
    )

    assert all_equal(
        trace.filter("Name", "not-in", ["MPI_Send", "MPI_Recv"]).events,
        trace.filter(Filter("Name", "not-in", ["MPI_Send", "MPI_Recv"])).events,
        trace.filter(Filter(expr='~`Name`.isin(["MPI_Send", "MPI_Recv"])')).events,
        trace.events[~trace.events["Name"].isin(["MPI_Send", "MPI_Recv"])],
    )

    assert all_equal(
        trace.filter("Timestamp (ns)", "between", ["130.52 ms", "135.67 ms"]).events,
        trace.filter("Timestamp (ns)", "between", [1.3052e08, 1.3567e08]).events,
        trace.filter(
            Filter("Timestamp (ns)", "between", [1.3052e08, 1.3567e08])
        ).events,
        trace.filter(
            Filter(
                expr="(`Timestamp (ns)` > 1.3052e+08) & (`Timestamp (ns)` < 1.3567e+08)"
            )
        ).events,
        trace.events[
            (trace.events["Timestamp (ns)"] > 1.3052e08)
            & (trace.events["Timestamp (ns)"] < 1.3567e08)
        ],
    )

    # Compound filter tests
    f1 = Filter("Timestamp (ns)", "between", ["130.52 ms", "135.67 ms"])
    f2 = Filter("Name", "in", ["MPI_Send", "MPI_Recv"])
    f3 = Filter("Process", "==", 0)

    # ensure logical NOT is done correctly
    assert all_equal(
        trace.filter(~f3).events,
        trace.filter(Filter(expr="~(`Process` == 0)")).events,
        trace.events[~(trace.events["Process"] == 0)],
    )

    # ensure logical AND is done correctly
    assert all_equal(
        trace.filter(f1 & f2 & f3).events,
        trace.filter(f1).filter(f2).filter(f3).events,
        trace.filter(f3).filter(f2).filter(f1).events,
    )

    assert set(trace.filter(f1 & f2 & f3).events.index) == set.intersection(
        set(trace.filter(f1).events.index),
        set(trace.filter(f2).events.index),
        set(trace.filter(f3).events.index),
    )

    # ensure logical OR is done correctly
    assert set(trace.filter(f1 | f2 | f3).events.index) == set.union(
        set(trace.filter(f1).events.index),
        set(trace.filter(f2).events.index),
        set(trace.filter(f3).events.index),
    )

    # ensure that they can be combined
    assert set(trace.filter((f1 & f2) | f3).events.index) == set.union(
        set.intersection(
            set(trace.filter(f1).events.index), set(trace.filter(f2).events.index)
        ),
        set(trace.filter(f3).events.index),
    )

    # keep_invalid test
    valid = trace.filter("Timestamp (ns)", ">", 1.33e08, keep_invalid=False).events
    invalid = trace.filter("Timestamp (ns)", ">", 1.33e08, keep_invalid=True).events

    # invalid is same as raw dataframe selection, since it doesn't remove rows
    assert all_equal(
        invalid, trace.events[trace.events["Timestamp (ns)"] > 1.33e08]
    )

    # number of enter/leave rows should match
    assert len(valid[valid["Event Type"] == "Enter"]) == len(
        valid[valid["Event Type"] == "Leave"]
    )

    # number of enter/leave rows should not match
    assert len(invalid[invalid["Event Type"] == "Enter"]) != len(
        invalid[invalid["Event Type"] == "Leave"]
    )

    # invalid always has more rows than valid
    assert len(invalid) > len(valid)
