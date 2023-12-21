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

cursor.close()
conn.close()
