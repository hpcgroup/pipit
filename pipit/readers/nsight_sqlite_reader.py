import numpy as np
import pandas as pd
import pipit.trace
import sqlite3


class NSightSQLiteReader:
    # Dictionary mapping trace type
    # (e.g. NVTX, CUDA API to SQL queries)
    _trace_queries = {
        "nvtx": [
            """
        SELECT
            start as Enter,
            end as Leave,
            'annotation' as type,
            IFNULL(text, StringIds.value) as "Name",
            (ne.globalTid >> 24) & 0x00FFFFFF AS "Process",
            ne.globalTid & 0x00FFFFFF AS "Thread",
            jsonText as meta
        FROM
            NVTX_EVENTS as ne
        LEFT JOIN StringIds
            ON StringIds.id = ne.textId
        WHERE
            -- Filter to only include range start/end and push/pop events
            ne.eventType in (59, 60)
        """
        ],
        "cuda_api": [
            """
        SELECT
            start as Enter,
            end as Leave,
            rname.value AS Name,
            (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
            cuda_api.globalTid & 0x00FFFFFF AS "Thread",
            correlationId As id,
            null as meta
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
        """
        ],
        "gpu_trace": [
            """
        SELECT
            cuda_gpu.start as Enter,
            cuda_gpu.end as Leave,
            cuda_gpu.deviceId as gpuId,
            value as Name,
            cuda_gpu.streamId,
            'kernel' as type,
            null as bytes,
            cuda_gpu.correlationId as id,
            (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
            null as meta
        FROM CUPTI_ACTIVITY_KIND_KERNEL as cuda_gpu
        JOIN StringIds
            ON cuda_gpu.shortName = StringIds.id
        JOIN CUPTI_ACTIVITY_KIND_RUNTIME as cuda_api
            ON cuda_gpu.correlationId = cuda_api.correlationId
        """,
        # TODO: reading these events are disabled for now since
        # nothing uses them ATM. Please remove if they have been
        # commented out like this for a while.
        #     """
        # SELECT
        #     cuda_memcpy.start as Enter,
        #     cuda_memcpy.end as Leave,
        #     cuda_memcpy.deviceId as gpuId,
        #     memcpy_labels.name as Name,
        #     cuda_memcpy.streamId,
        #     'cuda_memcpy' as type,
        #     bytes,
        #     cuda_memcpy.correlationId as id,
        #     (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
        #     null as meta
        # FROM CUPTI_ACTIVITY_KIND_MEMCPY as cuda_memcpy
        # JOIN ENUM_CUDA_MEMCPY_OPER as memcpy_labels
        #     ON cuda_memcpy.copyKind = memcpy_labels.id
        # JOIN CUPTI_ACTIVITY_KIND_RUNTIME as cuda_api
        #     ON cuda_memcpy.correlationId = cuda_api.correlationId
        # """,
        #     """
        # SELECT
        #     cuda_memset.start as Enter,
        #     cuda_memset.end as Leave,
        #     cuda_memset.deviceId as gpuId,
        #     memset_labels.name as Name,
        #     streamId,
        #     'cuda_memset' as type,
        #     bytes,
        #     cuda_memset.correlationId as id,
        #     (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
        #     null as meta
        # FROM CUPTI_ACTIVITY_KIND_MEMSET as cuda_memset
        # JOIN ENUM_CUDA_MEM_KIND as memset_labels
        #     ON cuda_memset.memKind = memset_labels.id
        # JOIN CUPTI_ACTIVITY_KIND_RUNTIME as cuda_api
        #     ON cuda_memset.correlationId = cuda_api.correlationId
        # """,
        #     """
        # SELECT
        #     cuda_sync.start as Enter,
        #     cuda_sync.end as Leave,
        #     cuda_sync.deviceId as gpuId,
        #     sync_labels.name as Name,
        #     cuda_sync.streamId,
        #     'cuda_sync' as type,
        #     null as bytes,
        #     cuda_sync.correlationId as id,
        #     (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
        #     null as meta
        # FROM CUPTI_ACTIVITY_KIND_SYNCHRONIZATION as cuda_sync
        # JOIN ENUM_CUPTI_SYNC_TYPE as sync_labels
        #     ON cuda_sync.syncType = sync_labels.id
        # JOIN CUPTI_ACTIVITY_KIND_RUNTIME as cuda_api
        #     ON cuda_sync.correlationId = cuda_api.correlationId
        # """,
            """
        SELECT
            cuda_graph.start as Enter,
            cuda_graph.end as Leave,
            cuda_graph.deviceId as gpuId,
            -- CUDA Graphs are not name-able, so we use their id
            -- instead
            'CUDA Graph ' || cuda_graph.graphId as Name,
            cuda_graph.streamId,
            'cuda_graph' as type,
            null as bytes,
            cuda_graph.correlationId as id,
            (cuda_api.globalTid >> 24) & 0x00FFFFFF AS "Process",
            null as meta
        FROM CUPTI_ACTIVITY_KIND_GRAPH_TRACE as cuda_graph
        JOIN CUPTI_ACTIVITY_KIND_RUNTIME as cuda_api
            ON cuda_graph.correlationId = cuda_api.correlationId
        """,
        ],
        # TODO: reading in all the gpu metrics takes up a lot of memory
        # We should figure out which ones we want exactly
        # "gpu_metrics": """
        #     SELECT GENERIC_EVENTS.rawTimestamp, typeId, data
        #     FROM GPU_METRICS
        #     LEFT JOIN GENERIC_EVENTS
        #     ON GENERIC_EVENTS.typeId = GPU_METRICS.typeId
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
            # Even nsight has separate analyses for CUDA API summary, etc.
            # We do need a way to compare multiple traces side by side, though

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
            gpu_trace_needed_tbls = [
                "CUPTI_ACTIVITY_KIND_RUNTIME",
                # TODO: remove if we decide we don't need these events
                # "CUPTI_ACTIVITY_KIND_MEMCPY",
                # CUPTI_ACTIVITY_KIND_MEMSET",
                # "CUPTI_ACTIVITY_KIND_SYNCHRONIZATION",
                "CUPTI_ACTIVITY_KIND_GRAPH_TRACE",
            ]

            for req_tbl, q in zip(
                gpu_trace_needed_tbls,
                NSightSQLiteReader._trace_queries["gpu_trace"],
                strict=True,
            ):
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
        trace_df = pd.concat(traces, axis=0)

        # Melt start/end columns into single event type column
        trace_df = pd.melt(
            trace_df,
            # These are the columns we don't want to melt
            # Columns not in here will be melted into a single column
            id_vars=[col for col in df.columns if col not in {"Enter", "Leave"}],
            value_vars=["Enter", "Leave"],
            var_name="Event Type",
            value_name="Timestamp (ns)",
        )

        # Convert to the pandas nullable dtypes
        # This will help preserve e.g. streamId as an
        # integer column with nulls instead of casting to
        # float64
        trace_df = trace_df.convert_dtypes()

        # Cache mapping
        trace_df["_matching_event"] = np.concatenate(
            [
                np.arange(len(trace_df) // 2, len(trace_df)),
                np.arange(0, len(trace_df) // 2),
            ]
        )
        # Convert to numpy before assignment otherwise pandas
        # will try to align indices, which will mess up order
        trace_df["_matching_timestamp"] = trace_df["Timestamp (ns)"][
            trace_df["_matching_event"]
        ].to_numpy()

        # Cannot use ignore_index = True since that breaks the
        # _matching_event col
        trace_df = trace_df.sort_values(by="Timestamp (ns)")

        if self.trace_types == ["gpu_trace"]:
            parallelism_levels = ["gpuId", "streamId"]
        elif self.trace_types == ["cuda_api"]:
            parallelism_levels = ["Process"]
        else:
            parallelism_levels = ["Process", "gpuId", "streamId"]

        trace = pipit.trace.Trace(None, trace_df, parallelism_levels=parallelism_levels)
        if self.create_cct:
            trace.create_cct()

        # Call match caller callee to recreate hierarchical
        # relationship between annotations
        trace._match_caller_callee()

        # Associate CUDA API calls with memory operations or
        # kernel launches
        # Note: looking at _match_caller_callee
        # _parent should point to the "Enter" event of the parent
        # _children also points to the "Enter" events of the children of 1 node

        enter_mask = trace_df["Event Type"] == "Enter"
        cuda_api_mask = trace_df["Trace Type"] == "cuda_api"
        calls_that_launch = (
            trace_df.loc[cuda_api_mask & enter_mask]
            .reset_index()
            .merge(
                trace_df.loc[~cuda_api_mask & enter_mask].reset_index(),
                on="id",
                how="inner",
            )
        )
        # Convert to numpy otherwise the index messes stuff up
        trace_df.loc[calls_that_launch["index_x"].to_numpy(), "_kernel_launch"] = (
            calls_that_launch["index_y"].to_numpy()
        )
        trace_df.loc[calls_that_launch["index_y"].to_numpy(), "_parent"] = (
            calls_that_launch["index_x"].to_numpy()
        )
        trace.events = trace_df
        return trace
