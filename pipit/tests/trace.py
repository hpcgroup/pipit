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


def test_time_profile(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    trace.calc_exc_time()

    time_profile = trace.time_profile(num_bins=61)

    # check length
    assert len(time_profile) == 61

    # check bin sizes
    exp_duration = trace.events["Timestamp (ns)"].max()
    exp_bin_size = exp_duration / 61
    bin_sizes = time_profile["bin_end"] - time_profile["bin_start"]

    assert np.isclose(bin_sizes, exp_bin_size).all()

    # check that sum of function contributions per bin equals bin duration
    exp_bin_total_duration = exp_bin_size * 2
    time_profile.drop(columns=["bin_start", "bin_end"], inplace=True)

    assert np.isclose(time_profile.sum(axis=1), exp_bin_total_duration).all()

    # check for each function that sum of exc time per bin equals total exc time
    total_exc_times = trace.events.groupby("Name")["time.exc"].sum()

    for column in time_profile:
        if column == "idle_time":
            continue

        assert np.isclose(time_profile[column].sum(), total_exc_times[column])

    # check normalization
    norm = trace.time_profile(num_bins=61, normalized=True)
    norm.drop(columns=["bin_start", "bin_end"], inplace=True)

    assert (time_profile / exp_bin_total_duration).equals(norm)

    # check against ground truth
    assert np.isclose(norm.loc[0]["MPI_Init"], 0.90695566)
    assert np.isclose(norm.loc[0]["MPI_Send"], 0.0)
    assert np.isclose(norm.loc[0]["MPI_Recv"], 0.0)
    assert np.isclose(norm.loc[0]["MPI_Finalize"], 0.0)

    assert np.isclose(norm.loc[1:57]["MPI_Init"], 1.0).all()
    assert np.isclose(norm.loc[1:57]["MPI_Send"], 0.0).all()
    assert np.isclose(norm.loc[1:57]["MPI_Recv"], 0.0).all()
    assert np.isclose(norm.loc[1:57]["MPI_Finalize"], 0.0).all()

    assert np.isclose(norm.loc[58]["MPI_Init"], 0.67562198)
    assert np.isclose(norm.loc[58]["MPI_Send"], 0.11226951)
    assert np.isclose(norm.loc[58]["MPI_Recv"], 0.09453169)
    assert np.isclose(norm.loc[58]["MPI_Finalize"], 0.0)

    assert np.isclose(norm.loc[59]["MPI_Init"], 0.0)
    assert np.isclose(norm.loc[59]["MPI_Send"], 0.30810867)
    assert np.isclose(norm.loc[59]["MPI_Recv"], 0.20585825)
    assert np.isclose(norm.loc[59]["MPI_Finalize"], 0.0)

    assert np.isclose(norm.loc[60]["MPI_Init"], 0.0)
    assert np.isclose(norm.loc[60]["MPI_Send"], 0.39240791)
    assert np.isclose(norm.loc[60]["MPI_Recv"], 0.25976402)
    assert np.isclose(norm.loc[60]["MPI_Finalize"], 0.04407401)
