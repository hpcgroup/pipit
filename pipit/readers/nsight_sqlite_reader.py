import sqlite3

"""Need to read from the following tables:
- CUPTI_ACTIVITY_KIND_RUNTIME
- CUPTI_ACTIVITY_KIND_KERNEL
- CUPTI_ACTIVITY_KIND_MEMSET
- CUPTI_ACTIVITY_KIND_MEMCPY
- NVTX_EVENTS
"""

conn = sqlite3.connect(
    "pipit/tests/data/saxpy-mpi-cuda_nsys_perlmutter0.sqlite"
)

cursor = conn.cursor()

nvtx_query = """
SELECT 
    start,
    end, 
    rname.value AS name,
    rname2.value AS rank,
    (ne.globalTid >> 24) & 0x00FFFFFF AS "PID",
    ne.globalTid & 0x00FFFFFF AS "TID"
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
"""

# ne.textId == rname.id
cursor.execute(nvtx_query)
nvtxpptrace_rows = cursor.fetchall()
nvtxpptrace_names = list(map(lambda x: x[0], cursor.description))
print(nvtxpptrace_names)

for row in nvtxpptrace_rows:
    print(row)
print(len(nvtxpptrace_rows))

kernel_query = """
SELECT 
    r.start AS "API Start:ts_ns",
    r.end - r.start AS "API Dur:dur_ns",
    CASE WHEN k.start - r.end >= 0 THEN r.end ELSE NULL END AS "Queue Start:ts_ns",
    CASE WHEN k.start - r.end >= 0 THEN k.start - r.end ELSE NULL END AS "Queue Dur:dur_ns",
    k.start AS "Kernel Start:ts_ns",
    k.end - k.start AS "Kernel Dur:dur_ns",
    max(r.end, k.end) - r.start AS "Total Dur:dur_ns",
    k.deviceId AS DevId,
    kname.value AS "Kernel Name",
    (r.globalTid >> 24) & 0x00FFFFFF AS PID,
    r.globalTid & 0x00FFFFFF AS TID
FROM
    CUPTI_ACTIVITY_KIND_KERNEL AS k
JOIN
    CUPTI_ACTIVITY_KIND_RUNTIME AS r
    USING( correlationId )
LEFT JOIN
    StringIds AS rname
    ON r.nameId == rname.id
LEFT JOIN
    StringIds AS kname
    ON kname.id == coalesce(k.demangledName, k.demangledName)
"""
cursor.execute(kernel_query)
kernel_rows = cursor.fetchall()
kernel_names = list(map(lambda x: x[0], cursor.description))
print(kernel_names)

for row in kernel_rows:
    print(row)
print(len(kernel_rows))

cudaapi_query = """
SELECT
    api.start AS "Start Time:ts_ns",
    api.end - api.start AS "Duration:dur_ns",
    CASE substr(nstr.value, -6, 2)
        WHEN '_v'THEN substr(nstr.value, 1, length(nstr.value)-6)
        ELSE nstr.value
    END AS "Name",
    api.returnValue AS "Result",
    api.correlationId AS "CorrID",
    -- (api.globalTid >> 40) & 0xFF AS "HWid",
    -- (api.globalTid >> 32) & 0xFF AS "VMid",
    (api.globalTid >> 24) & 0xFFFFFF AS "Pid",
    (api.globalTid      ) & 0xFFFFFF AS "Tid",
    tname.priority AS "T-Pri",
    tstr.value AS "Thread Name"
FROM
    CUPTI_ACTIVITY_KIND_RUNTIME AS api
LEFT OUTER JOIN
    StringIds AS nstr
    ON nstr.id == api.nameId
LEFT OUTER JOIN
    ThreadNames AS tname
    ON tname.globalTid == api.globalTid
LEFT OUTER JOIN
    StringIds AS tstr
    ON tstr.id == tname.nameId
ORDER BY 1
"""
cursor.execute(cudaapi_query)
cudaapi_rows = cursor.fetchall()
cudaapi_names = list(map(lambda x: x[0], cursor.description))
print(cudaapi_names)

for row in cudaapi_rows:
    print(row)
print(len(cudaapi_rows))

def cuda_callchains():
    # Check if the 'name' column already exists in the table
    cursor.execute("PRAGMA table_info(CUPTI_ACTIVITY_KIND_RUNTIME)")
    columns = cursor.fetchall()

    name_column_exists = any(column[1] == "name" for column in columns)

    if not name_column_exists:
        # Add the 'name' column
        cuda_callchains_query1 = """
        ALTER TABLE CUPTI_ACTIVITY_KIND_RUNTIME ADD COLUMN name TEXT;
        """
        cursor.execute(cuda_callchains_query1)

    cuda_callchains_query_update1 = """
    UPDATE CUPTI_ACTIVITY_KIND_RUNTIME SET name = (SELECT value FROM StringIds
    WHERE CUPTI_ACTIVITY_KIND_RUNTIME.nameId = StringIds.id);
    """
    cursor.execute(cuda_callchains_query_update1)

    cuda_callchains_query2 = """
    ALTER TABLE CUDA_CALLCHAINS ADD COLUMN symbolName TEXT;
    """
    cursor.execute(cuda_callchains_query2)

    cuda_callchains_query_update2 = """
    UPDATE CUDA_CALLCHAINS SET symbolName = (SELECT value FROM StringIds
    WHERE symbol = StringIds.id);
    """
    cursor.execute(cuda_callchains_query_update2)

    cuda_callchains_query3 = """
    SELECT globalTid % 0x1000000 AS TID,
    start, end, name, callchainId, stackDepth, symbolName
    FROM CUDA_CALLCHAINS
    JOIN CUPTI_ACTIVITY_KIND_RUNTIME
    ON callchainId == CUDA_CALLCHAINS.id
    ORDER BY callchainId, stackDepth;
    """

    """
    SELECT
        *
    FROM
        CUDA_CALLCHAINS as cc
    JOIN
        StringIds AS rname
        ON cc.symbol = rname.id
    """

    cursor.execute(cuda_callchains_query3)
    cuda_callchain_rows = cursor.fetchall()
    cuda_callchain_names = list(map(lambda x: x[0], cursor.description))
    print(cuda_callchain_names)

    for row in cuda_callchain_rows:
        print(row)


def sampling_callchains():
    cursor.execute("PRAGMA table_info(SAMPLING_CALLCHAINS)")
    columns = cursor.fetchall()
    symbolName_column_exists = any(column[1] == "symbolName" for column in columns)

    if not symbolName_column_exists:
        # Add the 'symbolName' column
        bottomup_alter1 = """
        ALTER TABLE SAMPLING_CALLCHAINS ADD COLUMN symbolName TEXT;
        """
        cursor.execute(bottomup_alter1)

    bottomup_update1 = """
    UPDATE SAMPLING_CALLCHAINS SET symbolName = (SELECT value FROM StringIds WHERE
    symbol = StringIds.id);
    """
    cursor.execute(bottomup_update1)

    bottomup_alter2 = """
    ALTER TABLE SAMPLING_CALLCHAINS ADD COLUMN moduleName TEXT;
    """
    cursor.execute(bottomup_alter2)

    bottomup_update2 = """
    UPDATE SAMPLING_CALLCHAINS SET moduleName = (SELECT value FROM StringIds WHERE
    module = StringIds.id);
    """
    cursor.execute(bottomup_update2)

    bottomup_select = """
    SELECT symbolName, moduleName, stackDepth, ROUND(100.0 * sum(cpuCycles) /
    (SELECT SUM(cpuCycles) FROM COMPOSITE_EVENTS), 2) AS selfTimePercentage
    FROM SAMPLING_CALLCHAINS
    LEFT JOIN COMPOSITE_EVENTS ON SAMPLING_CALLCHAINS.id == COMPOSITE_EVENTS.id
    WHERE stackDepth == 0
    GROUP BY symbol, module
    ORDER BY selfTimePercentage DESC
    """
    cursor.execute(bottomup_select)
    # cursor.execute("SELECT * FROM SAMPLING_CALLCHAINS")
    bottomup_rows = cursor.fetchall()
    bottomup_names = list(map(lambda x: x[0], cursor.description))
    print(bottomup_names)

    for row in bottomup_rows:
        print(row)


def cuda_callchains_memcpy():
    # Check if the 'name' column already exists in the table
    cursor.execute("PRAGMA table_info(CUPTI_ACTIVITY_KIND_MEMCPY)")
    columns = cursor.fetchall()

    name_column_exists = any(column[1] == "name" for column in columns)

    if not name_column_exists:
        # Add the 'name' column
        cuda_callchains_query1 = """
        ALTER TABLE CUPTI_ACTIVITY_KIND_MEMCPY ADD COLUMN name TEXT;
        """
        cursor.execute(cuda_callchains_query1)

    cuda_callchains_query_update1 = """
    UPDATE CUPTI_ACTIVITY_KIND_MEMCPY SET name = (SELECT value FROM StringIds
    WHERE CUPTI_ACTIVITY_KIND_MEMCPY.nameId = StringIds.id);
    """
    cursor.execute(cuda_callchains_query_update1)

    cuda_callchains_query2 = """
    ALTER TABLE CUDA_CALLCHAINS ADD COLUMN symbolName TEXT;
    """
    cursor.execute(cuda_callchains_query2)

    cuda_callchains_query_update2 = """
    UPDATE CUDA_CALLCHAINS SET symbolName = (SELECT value FROM StringIds
    WHERE symbol = StringIds.id);
    """
    cursor.execute(cuda_callchains_query_update2)

    cuda_callchains_query3 = """
    SELECT globalTid % 0x1000000 AS TID,
    start, end, name, callchainId, stackDepth, symbolName
    FROM CUDA_CALLCHAINS
    JOIN CUPTI_ACTIVITY_KIND_MEMCPY
    ON callchainId == CUDA_CALLCHAINS.id
    ORDER BY callchainId, stackDepth;
    """

    """
    SELECT
        *
    FROM
        CUDA_CALLCHAINS as cc
    JOIN
        StringIds AS rname
        ON cc.symbol = rname.id
    """

    cursor.execute(cuda_callchains_query3)
    cuda_callchain_rows = cursor.fetchall()
    cuda_callchain_names = list(map(lambda x: x[0], cursor.description))
    print(cuda_callchain_names)

    for row in cuda_callchain_rows:
        print(row)


cuda_callchains()
# cuda_callchains_memcpy()
sampling_callchains()

cursor.close()
conn.close()
