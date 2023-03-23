# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from pipit import Trace
import numpy as np
import pytest


@pytest.mark.xfail(
    reason="Allow this to fail until new HPCToolkit reader PR is merged."
)
def test_events(ping_pong_hpct_trace):
    ping_pong_hpct_trace = 'data/ping-pong-hpctoolkit'
    events_df = Trace.from_hpctoolkit(str(ping_pong_hpct_trace)).events

    # 2 processes in ping pong trace
    assert set(events_df["Process"]) == {0, 1}

    # event types for trace (instant events are program begin/end and mpi send/recv)
    assert set(events_df["Event Type"]) == set(["Enter", "Leave", "Loop Enter", 
                                                "Loop Leave"])

    # 89 Enter events in ping pong trace at process 0
    assert (
        len(
            events_df.loc[events_df["Process"] == 0].loc[
                events_df["Event Type"] == "Enter"]
        )
        == 117
    )

    # # 60 Enter events in ping pong trace at process 1
    assert (
        len(
            events_df.loc[events_df["Process"] == 1].loc[
                events_df["Event Type"] == "Enter"
            ]
        )
        == 88
    )

    # Same amount of Enter and Leave events in ping pong trace at process 1
    assert (
        len(
            events_df.loc[events_df["Process"] == 1].loc[
                events_df["Event Type"] == "Leave"
            ]
        )
        == len(
            events_df.loc[events_df["Process"] == 1].loc[
                events_df["Event Type"] == "Enter"
            ]
        )
    )
    
    # Same amount of Enter and Leave events in ping pong trace at process 0
    assert (
        len(
            events_df.loc[events_df["Process"] == 0].loc[
                events_df["Event Type"] == "Leave"
            ]
        )
        == len(
            events_df.loc[events_df["Process"] == 0].loc[
                events_df["Event Type"] == "Enter"
            ]
        )
    )

    # all event names in the trace
    assert set(events_df["Name"]) == {
        '<unknown procedure> 0x24680 [libpsm2.so.2.2]',
        'MPID_Finalize [libmpi.so.12.1.1]',
        'MPID_Recv [libmpi.so.12.1.1]',
        'MPI_Finalize',
        'PMPI_Finalize [libmpi.so.12.1.1]',
        'PMPI_Recv [libmpi.so.12.1.1]',
        'PMPI_Send [libmpi.so.12.1.1]',
        '__GI___munmap [libc-2.17.so]',
        '__GI___unlink [libc-2.17.so]',
        '__GI_process_vm_readv [libc-2.17.so]',
        'loop',
        'main',
        'main thread',
        'psm2_ep_close [libpsm2.so.2.2]',
        'psm2_mq_ipeek2 [libpsm2.so.2.2]',
        'psm2_mq_irecv2 [libpsm2.so.2.2]',
        'psm_dofinalize [libmpi.so.12.1.1]',
        'psm_progress_wait [libmpi.so.12.1.1]',
        'psm_recv [libmpi.so.12.1.1]',
        'psm_try_complete [libmpi.so.12.1.1]',
        'shm_unlink [librt-2.17.so]',
        'targ5030 [libpsm2.so.2.2]'}

    # Test correct number of MPI Send/Recv events
    mpi_send_df = events_df.loc[events_df["Name"].str.contains("PMPI_Send")].loc[
        events_df["Event Type"] == "Enter"
    ]
    mpi_recv_df = events_df.loc[events_df["Name"].str.contains("PMPI_Recv")].loc[
        events_df["Event Type"] == "Enter"
    ]

    # Process 0 has 6 MPI Sends and 5 MPI Recvs
    assert len(mpi_send_df.loc[events_df["Process"] == 0]) == 7
    assert len(mpi_recv_df.loc[events_df["Process"] == 0]) == 7

    # Process 1 has 5 MPI Sends and 5 MPI Recvs
    assert len(mpi_send_df.loc[events_df["Process"] == 1]) == 7
    assert len(mpi_recv_df.loc[events_df["Process"] == 1]) == 7

    # Timestamps should be sorted in increasing order
    assert (np.diff(events_df["Timestamp (ns)"]) >= 0).all()