import numpy as np
from pipit import Trace


def test_events(data_dir, ping_pong_otf2_trace):
    ping_pong_csv_trace = "data/ping-pong-csv.csv"
    trace_from_file = Trace.from_csv(str(ping_pong_csv_trace))

    # check that reading it as a string returns identical results
    csv_file = open(str(ping_pong_csv_trace))
    trace_from_str = Trace.from_csv(csv_file.read())
    csv_file.close()

    # also check that it's the same as the trace read by OTF2 reader
    otf2_trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    assert np.isclose(
        trace_from_str.events["Timestamp (ns)"],
        trace_from_file.events["Timestamp (ns)"],
    ).all()
    assert np.isclose(
        otf2_trace.events["Timestamp (ns)"],
        trace_from_file.events["Timestamp (ns)"],
    ).all()

    assert (trace_from_str.events["Name"] == trace_from_file.events["Name"]).all()
    assert (otf2_trace.events["Name"] == trace_from_file.events["Name"]).all()

    assert (trace_from_str.events["Thread"] == trace_from_file.events["Thread"]).all()
    assert (otf2_trace.events["Thread"] == trace_from_file.events["Thread"]).all()

    assert (trace_from_str.events["Process"] == trace_from_file.events["Process"]).all()
    assert (otf2_trace.events["Process"] == trace_from_file.events["Process"]).all()

    assert (
        trace_from_str.events["Event Type"] == trace_from_file.events["Event Type"]
    ).all()
    assert (
        otf2_trace.events["Event Type"] == trace_from_file.events["Event Type"]
    ).all()
