# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland.  See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from pipit import Trace
import numpy as np


def test_events(ping_pong_hpct_trace):
    events_df = Trace.from_hpctoolkit(str(ping_pong_hpct_trace)).events

    # 2 processes in ping pong trace
    assert set(events_df["Process"]) == {0, 1}

    # event types for trace (instant events are program begin/end and mpi send/recv)
    assert set(events_df["Event Type"]) == set(["Enter", "Leave"])

    # 89 Enter events in ping pong trace at process 0
    assert (
        len(
            events_df.loc[events_df["Process"] == 0].loc[
                events_df["Event Type"] == "Enter"
            ]
        )
        == 89
    )

    # 89 Leave events in ping pong trace at process 0
    assert (
        len(
            events_df.loc[events_df["Process"] == 0].loc[
                events_df["Event Type"] == "Leave"
            ]
        )
        == 89
    )

    # 60 Enter events in ping pong trace at process 1
    assert (
        len(
            events_df.loc[events_df["Process"] == 1].loc[
                events_df["Event Type"] == "Enter"
            ]
        )
        == 60
    )

    # 60 Leave events in ping pong trace at process 1
    assert (
        len(
            events_df.loc[events_df["Process"] == 1].loc[
                events_df["Event Type"] == "Leave"
            ]
        )
        == 60
    )

    # all event names in the trace
    assert set(events_df["Name"]) == set(
        [
            "<unknown procedure> 0xe087 [libpsm2.so.2.2]",
            "<unknown procedure> 0xd6a5 [libpsm2.so.2.2]",
            "<unknown procedure> 0x246e7 [libpsm2.so.2.2]",
            "<program root>",
            "psm2_mq_irecv2",
            "<unknown procedure> 0x245c0 [libpsm2.so.2.2]",
            "<unknown procedure> 0xc91c [libpsm2.so.2.2]",
            "<unknown procedure> 0xda5d [libpsm2.so.2.2]",
            "__GI_process_vm_readv",
            "PMPI_Recv",
            "main",
            "psm_progress_wait",
            "<no activity>",
            "<unknown procedure> 0x246c7 [libpsm2.so.2.2]",
            "<unknown procedure> 0x24680 [libpsm2.so.2.2]",
            "psm_try_complete",
            "<unknown procedure> 0x64d4 [libpsm2.so.2.2]",
            "PMPI_Send",
            "psm2_mq_ipeek2",
            "<unknown procedure> 0xc850 [libpsm2.so.2.2]",
            "psm_recv",
            "MPID_Recv",
        ]
    )

    # Test correct number of MPI Send/Recv events
    mpi_send_df = events_df.loc[events_df["Name"] == "PMPI_Send"].loc[
        events_df["Event Type"] == "Enter"
    ]
    mpi_recv_df = events_df.loc[events_df["Name"] == "PMPI_Recv"].loc[
        events_df["Event Type"] == "Enter"
    ]

    # Process 0 has 6 MPI Sends and 5 MPI Recvs
    assert len(mpi_send_df.loc[events_df["Process"] == 0]) == 6
    assert len(mpi_recv_df.loc[events_df["Process"] == 0]) == 5

    # Process 1 has 5 MPI Sends and 5 MPI Recvs
    assert len(mpi_send_df.loc[events_df["Process"] == 1]) == 5
    assert len(mpi_recv_df.loc[events_df["Process"] == 1]) == 5

    # Timestamps should be sorted in increasing order
    assert (np.diff(events_df["Timestamp (ns)"]) >= 0).all()
