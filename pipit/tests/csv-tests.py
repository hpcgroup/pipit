import numpy as np
from pipit import Trace


def test_events(data_dir, ping_pong_otf2_trace):
    ping_pong_csv_trace = "data/ping-pong-csv.csv"
    trace_from_file = Trace.from_csv(str(ping_pong_csv_trace))

    # check that reading it as a string returns identical results
    csv_file = open(str(ping_pong_csv_trace))
    trace_from_str = Trace.from_csv(csv_file.read())
    csv_file.close()

    assert trace_from_file.events == trace_from_str.events

    # check that it's the same as the trace read by OTF2 reader
    otf2_trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    assert otf2_trace.events == trace_from_file.events
