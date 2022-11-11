# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
from pipit import Trace


def test_events(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    events_df = trace.events

    # 108 total events in ping pong trace
    assert len(events_df) == 108

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
        ]
    )

    # 8 sends per rank, so 16 sends total -> 32 including both enter and leave rows
    assert len(events_df.loc[events_df["Name"] == "MPI_Send"]) == 32

    assert (
        len(set(events_df["Process"])) == len(set(events_df["Thread"])) == 2
    )  # 2 ranks for ping pong trace

    assert (
        len(events_df.loc[events_df["Process"] == 0])
        == len(events_df.loc[events_df["Thread"] == 0])
        == 54
    )  # 54 events per rank

    # timestamps should be sorted in increasing order
    assert (np.diff(events_df["Timestamp (ns)"]) > 0).all()


def test_definitions(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))
    definitions_df = trace.definitions

    assert len(definitions_df) == 229

    # 17 unique definition types in trace
    assert len(set(definitions_df["Definition Type"])) == 17

    # 2 ranks, so 2 location definitions in the trace
    assert len(definitions_df.loc[definitions_df["Definition Type"] == "Location"]) == 2

    # communicator should evidently be present in the ping pong trace definitions
    assert "Comm" in set(definitions_df["Definition Type"])
