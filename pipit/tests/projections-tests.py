# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland.  See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from pipit import Trace


def test_events(data_dir, ping_pong_projections_trace):
    trace = Trace.from_projections(str(ping_pong_projections_trace))
    events_df = trace.events

    # The projections trace has 2 PEs
    assert set(events_df["Process"]) == {0, 1}

    # event types for trace
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

    # PE 1 has 68 create events
    assert (
        len(events_df.loc[events_df["Process"] == 1].loc[events_df["Name"] == "Create"])
        == 68
    )
    # PE 0 has 77 create events
    assert (
        len(events_df.loc[events_df["Process"] == 0].loc[events_df["Name"] == "Create"])
        == 77
    )

    # PE0 has 161 Begin Processing Events
    len(
        events_df.loc[events_df["Process"] == 0]
        .loc[events_df["Event Type"] == "Enter"]
        .loc[events_df["Name"] == "Processing"]
    ) == 161
    # PE0 has 146 Begin Processing Events
    len(
        events_df.loc[events_df["Process"] == 1]
        .loc[events_df["Event Type"] == "Enter"]
        .loc[events_df["Name"] == "Processing"]
    ) == 146

    # Each log file starts/ends with a Computation Event
    assert events_df.loc[events_df["Process"] == 1].iloc[0]["Name"] == "Computation"
    assert events_df.loc[events_df["Process"] == 1].iloc[-1]["Name"] == "Computation"

    assert events_df.loc[events_df["Process"] == 0].iloc[0]["Name"] == "Computation"
    assert events_df.loc[events_df["Process"] == 0].iloc[-1]["Name"] == "Computation"
