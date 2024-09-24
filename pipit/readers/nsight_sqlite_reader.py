import numpy as np
import pandas as pd

import pipit.trace
from pipit.graph import Graph, Node
import sqlite3

"""Need to read from the following tables:
- CUPTI_ACTIVITY_KIND_RUNTIME
- CUPTI_ACTIVITY_KIND_KERNEL
- CUPTI_ACTIVITY_KIND_MEMSET
- CUPTI_ACTIVITY_KIND_MEMCPY
- NVTX_EVENTS

Times are in nanoseconds
"""
class NSightSQLiteReader:
    # Dictionary mapping trace type
    # (e.g. NVTX,
    _trace_queries = {
        "nvtx": ["""
        SELECT
            start as Enter,
            end as Leave,
            rname.value AS Name,
            (ne.globalTid >> 24) & 0x00FFFFFF AS "Process",
            ne.globalTid & 0x00FFFFFF AS "Thread"
        FROM
            NVTX_EVENTS as ne
        JOIN ThreadNames AS tname
            ON ne.globalTid == tname.globalTid
        JOIN
            StringIds AS rname
            ON ne.textId = rname.id
        JOIN
            StringIds AS rname2
            ON tname.nameId = rname2.id
        """],
        "cuda_api": ["""
        SELECT 
            start as Enter,
            end as Leave, 
            rname.value AS Name,
            (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
            cuda_api.globalTid & 0x00FFFFFF AS "Thread"
        FROM
            CUPTI_ACTIVITY_KIND_RUNTIME as cuda_api
        JOIN ThreadNames AS tname
            ON cuda_api.globalTid == tname.globalTid
        JOIN
            StringIds AS rname
            ON cuda_api.nameId = rname.id
        JOIN
            StringIds AS rname2
            ON tname.nameId = rname2.id
        """],
        "gpu_trace": ["""
        SELECT 
            start as Enter,
            end as Leave,
            value as Name, 
            streamId,
            'kernel' as type,
            null as bytes
        FROM CUPTI_ACTIVITY_KIND_KERNEL as cuda_gpu
        JOIN StringIds
            ON cuda_gpu.shortName = StringIds.id
        """,
        """
        SELECT 
            start as Enter, 
            end as Leave, 
            memcpy_labels.name as Name,
            streamId, 
            memcpy_labels.name as type, 
            bytes
        FROM CUPTI_ACTIVITY_KIND_MEMCPY as cuda_memcpy
        JOIN ENUM_CUDA_MEMCPY_OPER as memcpy_labels
            ON  cuda_memcpy.copyKind = memcpy_labels.id
        """,
        """
        SELECT 
            start as Enter, 
            end as Leave, 
            memset_labels.name as Name,
            streamId,
            memset_labels.name as type, 
            bytes
        FROM CUPTI_ACTIVITY_KIND_MEMSET as cuda_memset
        JOIN ENUM_CUDA_MEM_KIND as memset_labels
            ON cuda_memcpy.copyKind = memset_labels.id
        """,]
        # TODO: reading in all the gpu metrics takes up a lot of memory
        # We should figure out which ones we want exactly
        # "gpu_metrics": """
        # SELECT GENERIC_EVENTS.timestamp, data
        # FROM GPU_METRICS
        # LEFT JOIN GENERIC_EVENTS
        # ON GENERIC_EVENTS.typeId = GPU_METRICS.typeId
        # """
    }
    def __init__(self, filepath, create_cct=False, trace_types="all") -> None:
        self.conn = sqlite3.connect(filepath)
        self.create_cct = create_cct
        # Get all the table names that exist
        # Sometimes, things like the GPU metrics and stuff might not
        # exist
        get_tables_query = """
        SELECT name FROM sqlite_master WHERE type='table'
        """
        self.table_names = set(pd.read_sql_query(get_tables_query, self.conn).squeeze())
        self.trace_queries = NSightSQLiteReader._trace_queries.copy()
        if trace_types == "all":
            # Some traces (their tables, e.g. NVTX_EVENTS) may not always be present
            # in the sqlite db
            # Make sure that all tables that we read in queries are accounted for here
            self.trace_types = []
            if "NVTX_EVENTS" in self.table_names:
                self.trace_types.append("nvtx")
            if "CUPTI_ACTIVITY_KIND_RUNTIME" in self.table_names:
                self.trace_types.append("cuda_api")
                self.trace_types.append("gpu_trace")

            # GPU metrics are disabled, see comment above
            # if "GPU_METRICS" in self.table_names:
            #     self.trace_types.append("gpu_metrics")
        else:
            self.trace_types = trace_types

        if "gpu_trace" in self.trace_types:
            # Check for existance of CUDA_ACTIVITY_KIND_MEMCPY/
            # CUDA_ACTIVITY_KIND_MEMSET since those can sometimes not exist

            gpu_trace_qs = []
            gpu_trace_needed_tbls = ["CUPTI_ACTIVITY_KIND_RUNTIME", "CUPTI_ACTIVITY_KIND_MEMCPY",
                                     "CUPTI_ACTIVITY_KIND_MEMSET"]

            for req_tbl, q in zip(gpu_trace_needed_tbls, NSightSQLiteReader._trace_queries["gpu_trace"]):
                if req_tbl in self.table_names:
                    gpu_trace_qs.append(q)
            self.trace_queries["gpu_trace"] = gpu_trace_qs

    def read(self) -> pipit.trace.Trace:
        traces = []

        for typ in self.trace_types:
            dfs = []
            for q in self.trace_queries[typ]:
                dfs.append(pd.read_sql_query(q, con=self.conn))
            df = pd.concat(dfs, axis=0)
            df["Trace Type"] = typ
            traces.append(df)

        # concat traces together row wise
        # TODO: maybe we should keep these partitioned
        # not sure if pipit is able to handle this currently, though
        trace_df = pd.concat(traces, axis=0)

        # Melt start/end columns into single event type column
        trace_df = pd.melt(trace_df,
                # These are the columns we don't want to melt
                # Columns not in here will be melted into a single column
                id_vars=set(df.columns) - {"Enter", "Leave"},
                value_vars=["Enter", "Leave"],
                var_name="Event Type",
                value_name="Timestamp (ns)")
        # Cache mapping
        trace_df["_matching_event"] = np.concatenate([np.arange(len(trace_df) // 2, len(trace_df)), np.arange(0, len(trace_df) // 2)])
        # Convert to numpy before assignment otherwise pandas
        # will try to align indices, which will mess up order
        trace_df['_matching_timestamp'] = trace_df["Timestamp (ns)"][trace_df["_matching_event"]].to_numpy()

        # NSight systems does provide us with a backtrace (but it is not very useful)
        # since we can't correlate nvtx events with stuff like CUDA API calls
        # TODO: We might be able to show nested nvtx ranges, though
        trace_df["_depth"] = 0
        trace_df["_parent"] = None
        trace_df["_children"] = None
        trace = pipit.trace.Trace(None, trace_df)
        if self.create_cct:
            trace.create_cct()
        return trace