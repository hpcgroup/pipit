# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import os
import shutil
from glob import glob

import pytest


@pytest.fixture
def data_dir():
    """Return path to the top-level data directory for tests."""
    parent = os.path.dirname(__file__)
    return os.path.join(parent, "data")


@pytest.fixture
def ping_pong_hpct_trace(data_dir, tmpdir):
    """Builds a temporary directory containing the ping-pong traces."""
    hpct_db_dir = os.path.join(data_dir, "ping-pong-hpctoolkit")

    for f in glob(os.path.join(str(hpct_db_dir), "*.db")):
        shutil.copy(f, str(tmpdir))
    shutil.copy(os.path.join(hpct_db_dir, "experiment.xml"), str(tmpdir))

    return tmpdir

@pytest.fixture
def ping_pong_projections_trace(data_dir, tmpdir):
    """Builds a temporary directory containing the ping-pong traces."""
    hpct_db_dir = os.path.join(data_dir, "ping-pong-projections")

    shutil.copy(os.path.join(hpct_db_dir, "pingpong.prj.sts"), str(tmpdir))
    shutil.copy(os.path.join(hpct_db_dir, "pingpong.prj.0.log.gz"), str(tmpdir))
    shutil.copy(os.path.join(hpct_db_dir, "pingpong.prj.1.log.gz"), str(tmpdir))

    return tmpdir


@pytest.fixture
def ping_pong_projections_trace(data_dir, tmpdir):
    """Builds a temporary directory containing the ping-pong traces."""
    hpct_db_dir = os.path.join(data_dir, "ping-pong-projections")

    shutil.copy(os.path.join(hpct_db_dir, "pingpong.prj.sts"), str(tmpdir))
    shutil.copy(os.path.join(hpct_db_dir, "pingpong.prj.0.log.gz"), str(tmpdir))
    shutil.copy(os.path.join(hpct_db_dir, "pingpong.prj.1.log.gz"), str(tmpdir))

    return tmpdir


@pytest.fixture
def ping_pong_otf2_trace(data_dir, tmpdir):
    """Builds a temporary directory containing the ping-pong traces."""
    otf2_dir = os.path.join(data_dir, "ping-pong-otf2")

    shutil.copytree(os.path.join(str(otf2_dir), "traces"), str(tmpdir) + "/traces")
    shutil.copy(os.path.join(str(otf2_dir), "scorep.cfg"), str(tmpdir))
    shutil.copy(os.path.join(str(otf2_dir), "traces.def"), str(tmpdir))
    shutil.copy(os.path.join(str(otf2_dir), "traces.otf2"), str(tmpdir))

    return tmpdir
