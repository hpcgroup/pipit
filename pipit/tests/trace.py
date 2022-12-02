# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest
import numpy as np
from pipit import Trace


@pytest.mark.xfail(reason="Allow this to fail until otf2 has a pip package.")
def test_comm_matrix(ping_pong_otf2_trace):
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
