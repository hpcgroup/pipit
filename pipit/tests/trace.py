# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest
from pipit import Trace


@pytest.mark.xfail(reason="Allow this to fail until otf2 has a pip package.")
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
