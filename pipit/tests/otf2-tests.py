# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
from pipit import Trace


def test_events(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    events_df = trace.events

    # 120 total events in ping pong trace
    assert len(events_df) == 120

    # event types for trace (instant events are program begin/end and mpi send/recv)
    assert set(events_df["Event Type"]) == set(["Enter", "Instant", "Leave"])

    # all event names in the trace
    assert set(events_df["Name"]) == set(
        [
            "ProgramBegin",
            "ProgramEnd",
            "MPI_Send",
            "MPI_Recv",
            "MpiSend",
            "MpiRecv",
            "MPI_Init",
            "MPI_Finalize",
            "MPI_Comm_rank",
            "MPI_Comm_size",
            "int main(int, char**)",
        ]
    )

    # 8 sends per rank, so 16 sends total -> 32 including both enter and leave rows
    assert len(events_df.loc[events_df["Name"] == "MPI_Send"]) == 32

    assert len(set(events_df["Process"])) == 2  # 2 ranks for ping pong trace

    assert len(set(events_df["Thread"])) == 1  # 1 thread per rank

    assert len(events_df.loc[events_df["Process"] == 0]) == 60  # 60 events per rank

    assert (
        len(events_df.loc[events_df["Thread"] == 0]) == 120
    )  # all 120 events associated with the main thread

    # timestamps should be sorted in increasing order
    assert (np.diff(events_df["Timestamp (ns)"]) > 0).all()


def test_definitions(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    definitions_df = trace.definitions

    assert len(definitions_df) == 533

    # 17 unique definition types in trace
    assert len(set(definitions_df["Definition Type"])) == 17

    # 2 ranks, so 2 location definitions in the trace
    assert len(definitions_df.loc[definitions_df["Definition Type"] == "Location"]) == 2

    # communicator should evidently be present in the ping pong trace definitions
    assert "Comm" in set(definitions_df["Definition Type"])
