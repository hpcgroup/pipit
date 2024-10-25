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


def test_comm_over_time(data_dir, ping_pong_otf2_trace):
    ping_pong = Trace.from_otf2(str(ping_pong_otf2_trace))

    hist, edges = ping_pong.comm_over_time(output="size", message_type="send", bins=5)

    assert len(edges) == 6
    assert all(hist[0:3] == 0)
    assert hist[4] == 4177920 * 2

    hist, edges = ping_pong.comm_over_time(
        output="count", message_type="receive", bins=5
    )

    assert len(edges) == 6
    assert all(hist[0:3] == 0)
    assert hist[4] == 8 * 2


def test_comm_by_process(data_dir, ping_pong_otf2_trace):
    ping_pong = Trace.from_otf2(str(ping_pong_otf2_trace))

    sizes = ping_pong.comm_by_process()

    assert sizes.loc[0]["Sent"] == 4177920
    assert sizes.loc[0]["Received"] == 4177920
    assert sizes.loc[1]["Sent"] == 4177920
    assert sizes.loc[1]["Received"] == 4177920

    counts = ping_pong.comm_by_process(output="count")

    assert counts.loc[0]["Sent"] == 8
    assert counts.loc[0]["Received"] == 8
    assert counts.loc[1]["Sent"] == 8
    assert counts.loc[1]["Received"] == 8


def test_match_charm_messages(ping_pong_projections_trace):
    trace = Trace.from_projections(str(ping_pong_projections_trace))
    trace._match_charm_messages()

    df = trace.events

    receive_events = df[df["Attributes"].apply(
        lambda x: False if not x else ("Entry Type" in x) and (x["Entry Type"] == "Processing")
    )]

    # Filter out unmatched receive events
    receive_events = receive_events.loc[receive_events["_matching_message"].notnull()]

    receive_indices = list(receive_events.index)
    receive_matching_message = list(receive_events["_matching_message"])
    receive_process = list(receive_events["Process"])
    

    for i in range(len(receive_matching_message)):
        receive_index = receive_indices[i]
        send_index = receive_matching_message[i]

        if send_index != 200:
            corresponding_send = df.iloc[send_index]

            # Ensure that, for each receive event, the corresponding send event has
            # that receive event index in its matching message column
            #
            # Testing if the distance between the two DataFrame indices is <= 17 accounts
            # accounts for some weirdness in the trace where some message receives are divided into
            # several enter events. Most distances will be 0 (equal indices).
            assert abs(corresponding_send["_matching_message"] - receive_index) <= 17

            # With a few exceptions, messages send and receive on opposite processes
            exceptions = [541, 549, 553, 1120, 1124, 1305, 1306, 1320, 1882, 1893, 1894]
            if receive_index >= 360 and receive_index not in exceptions:
                assert receive_process[i] != corresponding_send["Process"]


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

    # All events in ping pong trace except main are leaves in the call tree,
    # so the leave row occurs immediately after the enter. The below assertions
    # test this.
    for i in range(len(rank_0_df)):
        if (
            rank_0_df["Event Type"].iloc[i] == "Enter"
            and rank_0_df["Name"].iloc[i] != "int main(int, char**)"
        ):
            # the matching event and timestamp for enter rows
            # should occur right after (ex: (Enter: 45, Leave: 46))
            assert rank_0_matching_indices[i] == rank_0_indices[i + 1]
            assert rank_0_matching_timestamps[i] == rank_0_timestamps[i + 1]
        elif rank_0_df["Name"].iloc[i] != "int main(int, char**)":
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
        if (
            rank_1_df["Event Type"].iloc[i] == "Enter"
            and rank_1_df["Name"].iloc[i] != "int main(int, char**)"
        ):
            assert rank_1_matching_indices[i] == rank_1_indices[i + 1]
            assert rank_1_matching_timestamps[i] == rank_1_timestamps[i + 1]
        elif rank_1_df["Name"].iloc[i] != "int main(int, char**)":
            assert rank_1_matching_indices[i] == rank_1_indices[i - 1]
            assert rank_1_matching_timestamps[i] == rank_1_timestamps[i - 1]

    # Checks that the Matching Indices and Timestamps for the Enter rows are
    # greater than their values
    assert (
        np.array(df.loc[df["Event Type"] == "Enter"]["_matching_event"])
        > np.array(df.loc[df["Event Type"] == "Enter"].index)
    ).all()
    assert (
        np.array(df.loc[df["Event Type"] == "Enter"]["_matching_timestamp"])
        > np.array(df.loc[df["Event Type"] == "Enter"]["Timestamp (ns)"])
    ).all()


def test_match_caller_callee(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    trace._match_caller_callee()

    df = trace.events

    # nodes with a parent = 40
    assert len(df.loc[df["_parent"].notnull()]) == 40

    # nodes with children = 2
    assert len(df.loc[df["_children"].notnull()]) == 2


def test_time_profile(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    trace.calc_exc_metrics(["Timestamp (ns)"])

    time_profile = trace.time_profile(num_bins=62)

    # check length
    assert len(time_profile) == 62

    # check bin sizes
    exp_duration = trace.events["Timestamp (ns)"].max()
    exp_bin_size = exp_duration / 62
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
    norm = trace.time_profile(num_bins=62, normalized=True)
    norm.drop(columns=["bin_start", "bin_end"], inplace=True)

    assert (time_profile / exp_bin_total_duration).equals(norm)

    # check against ground truth
    # generated using Vampir's Function Summary chart (step size=16)
    assert np.isclose(norm.loc[0]["int main(int, char**)"], 0.00299437)
    assert np.isclose(norm.loc[0]["MPI_Init"], 0.93999815)
    assert np.isclose(norm.loc[0]["MPI_Comm_size"], 0.0)
    assert np.isclose(norm.loc[0]["MPI_Comm_rank"], 0.0)
    assert np.isclose(norm.loc[0]["MPI_Send"], 0.0)
    assert np.isclose(norm.loc[0]["MPI_Recv"], 0.0)
    assert np.isclose(norm.loc[0]["MPI_Finalize"], 0.0)

    assert np.isclose(norm.loc[1:59]["int main(int, char**)"], 0.0).all()
    assert np.isclose(norm.loc[1:59]["MPI_Init"], 1.0).all()
    assert np.isclose(norm.loc[1:59]["MPI_Comm_size"], 0.0).all()
    assert np.isclose(norm.loc[1:59]["MPI_Comm_rank"], 0.0).all()
    assert np.isclose(norm.loc[1:59]["MPI_Send"], 0.0).all()
    assert np.isclose(norm.loc[1:59]["MPI_Recv"], 0.0).all()
    assert np.isclose(norm.loc[1:59]["MPI_Finalize"], 0.0).all()

    assert np.isclose(norm.loc[60]["int main(int, char**)"], 0.39464799)
    assert np.isclose(norm.loc[60]["MPI_Init"], 0.14843661)
    assert np.isclose(norm.loc[60]["MPI_Send"], 0.24594134)
    assert np.isclose(norm.loc[60]["MPI_Recv"], 0.21017099)
    assert np.isclose(norm.loc[60]["MPI_Comm_size"], 0.00046047)
    assert np.isclose(norm.loc[60]["MPI_Comm_rank"], 0.00034261)
    assert np.isclose(norm.loc[60]["MPI_Finalize"], 0.0)

    assert np.isclose(norm.loc[61]["int main(int, char**)"], 0.43560727)
    assert np.isclose(norm.loc[61]["MPI_Init"], 0.0)
    assert np.isclose(norm.loc[61]["MPI_Send"], 0.29640222)
    assert np.isclose(norm.loc[61]["MPI_Recv"], 0.24300865)
    assert np.isclose(norm.loc[61]["MPI_Comm_size"], 0.0)
    assert np.isclose(norm.loc[61]["MPI_Comm_rank"], 0.0)
    assert np.isclose(norm.loc[61]["MPI_Finalize"], 0.01614835)
