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
        "nvtx": """
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
        """,
        "cuda_api": """
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
        """,

    }
    def __init__(self, filepath, create_cct=False, trace_types="all") -> None:
        self.filepath = filepath
        self.create_cct = create_cct
        # Get all the table names that exist
        # Sometimes, things like the GPU metrics and stuff might not
        # exist
        get_tables_query = """
        SELECT name FROM sqlite_master WHERE type='table'
        """
        self.table_names = set(pd.read_sql_query(get_tables_query, conn).squeeze())
        if trace_types == "all":
            # TODO: some traces (their tables) may not always be present
            # in the sqlite db
            # (e.g. this might affect the nvtx stuff?)
            self.trace_types = ["nvtx", "cuda_api"]
        else:
            self.trace_types = trace_types

    def read(self) -> pipit.trace.Trace:
        con = sqlite3.connect(self.filepath)
        traces = []

        for typ in self.trace_types:
            df = pd.read_sql_query(NSightSQLiteReader._trace_queries[typ], con=con)
            # add column so we know which trace type it came from
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
                id_vars=["Name", "Process", "Thread", "Trace Type"],
                var_name="Event Type",
                value_name="Timestamp (ns)")
        trace = pipit.trace.Trace(None, trace_df)
        if self.create_cct:
            trace.create_cct()
        return trace