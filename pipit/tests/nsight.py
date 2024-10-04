import os
import pandas as pd
import pipit
import pytest
import numpy as np

# compare read in traces against
# "golden" files containing the
# correct output

# TODO: use a file format (e.g. parquet)
# that preserves dtypes

def test_gpu_trace(data_dir):
    res_trace = pipit.Trace.from_nsight_sqlite(
        os.path.join(data_dir, "cloverleaf-nsys/perlmutter-cloverleaf-cuda.sqlite"),
        trace_types=["gpu_trace"]
    )
    exp_events = pd.read_csv(os.path.join(data_dir, "cloverleaf-nsys/cloverleaf_gpu_trace.csv"), dtype={
        "bytes": "Int64",
        "_parent": "object",
        "_children": "object"
    })
    pd.testing.assert_frame_equal(res_trace.events, exp_events)


def test_cuda_api_trace(data_dir):
    res_trace = pipit.Trace.from_nsight_sqlite(
        os.path.join(data_dir, "cloverleaf-nsys/perlmutter-cloverleaf-cuda.sqlite"),
        trace_types=["cuda_api"]
    )
    exp_events = pd.read_csv(os.path.join(data_dir, "cloverleaf-nsys/cloverleaf-cuda_api_trace.csv"), dtype={
        "_parent": "object",
        "_children": "object"
    })
    pd.testing.assert_frame_equal(res_trace.events, exp_events)
