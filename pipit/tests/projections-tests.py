# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from pipit import Trace


def test_events(data_dir, ping_pong_projections_trace):
    trace = Trace.from_projections(str(ping_pong_projections_trace))
    events_df = trace.events

    # 108 total events in ping pong trace
    assert len(events_df) == 1897

    # The projections trace has 2 PEs
    assert set(events_df["Process ID"]) == {0, 1}

    # event types for trace (instant events are program begin/end and mpi send/recv)
    assert set(events_df["Event Type"]) == {"Enter", "Instant", "Leave"}

    # all event names in the trace
    assert set(events_df["Name"]) == {
        "Idle",
        "Create",
        "Processing",
        "Computation",
        "Unpack",
        "Pack",
    }

    # 145 Create events
    assert len(events_df.loc[events_df["Name"] == "Create"]) == 145

    # Each log file starts/ends with a Computation Event
    assert events_df.loc[events_df["Process ID"] == 1].iloc[0]['Name'] == "Computation"
    assert events_df.loc[events_df["Process ID"] == 1].iloc[-1]['Name'] == "Computation"

    assert events_df.loc[events_df["Process ID"] == 0].iloc[0]['Name'] == "Computation"
    assert events_df.loc[events_df["Process ID"] == 0].iloc[-1]['Name'] == "Computation"
