from pipit import Trace


def all_equal(*dfs):
    return all([dfs[0].equals(df) for df in dfs])


def test_eval(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    # Test each operator
    assert all_equal(
        trace._eval("Process", "==", 0),
        trace._eval(expr="`Process` == 0"),
        trace.events["Process"] == 0,
    )
    assert all_equal(
        trace._eval("Name", "!=", "MPI_Init"),
        trace._eval(expr="`Name` != 'MPI_Init'"),
        trace.events["Name"] != "MPI_Init",
    )
    assert all_equal(
        trace._eval("Timestamp (ns)", "<", "500 ns"),
        trace._eval("Timestamp (ns)", "<", 500),
        trace._eval(expr="`Timestamp (ns)` < 500"),
        trace.events["Timestamp (ns)"] < 500,
    )
    assert all_equal(
        trace._eval("Timestamp (ns)", ">", "199.6 ms"),
        trace._eval("Timestamp (ns)", ">", 1.996e8),
        trace._eval(expr="`Timestamp (ns)` > 1.996e8"),
        trace.events["Timestamp (ns)"] > 1.996e8,
    )
    assert all_equal(
        trace._eval("Name", "in", ["MPI_Send", "MPI_Recv"]),
        trace._eval(expr="`Name`.isin(['MPI_Send', 'MPI_Recv'])"),
        trace.events["Name"].isin(["MPI_Send", "MPI_Recv"]),
    )
    assert all_equal(
        trace._eval("Name", "not-in", ["MPI_Send", "MPI_Recv"]),
        trace._eval(expr="~(`Name`.isin(['MPI_Send', 'MPI_Recv']))"),
        ~trace.events["Name"].isin(["MPI_Send", "MPI_Recv"]),
    )
    assert all_equal(
        trace._eval("Timestamp (ns)", "between", ["50 ns", "199.6 ms"]),
        (trace.events["Timestamp (ns)"] > 50)
        & (trace.events["Timestamp (ns)"] < 1.996e8),
    )

    # Test logical operators NOT, AND, and OR
    from pipit.selection import Filter

    f1 = Filter("Timestamp (ns)", "between", ["130.52 ms", "136.57 ms"])
    f2 = Filter("Name", "in", ["MPI_Send", "MPI_Recv"])
    f3 = Filter("Process", "==", 0)

    assert all_equal(trace._eval(~f3), ~trace._eval(f3))

    assert all_equal(
        trace._eval(f1 & f2 & f3),
        trace._eval(f1) & trace._eval(f2) & trace._eval(f3),
    )
    assert all_equal(
        trace._eval(f1 | f2 | f3),
        trace._eval(f1) | trace._eval(f2) | trace._eval(f3),
    )


def test_filter(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    assert all_equal(
        trace.filter("Process", "==", 0).definitions,
        trace.definitions,
    )

    assert all_equal(
        trace.filter("Process", "==", 0).events,
        trace.events[trace.events["Process"] == 0],
    )

    # Test that "between" returns functions that span the time range
    assert (
        "int main(int, char**)"
        in trace.filter("Timestamp (ns)", "between", [3.50e05, 3.51e5])
        .events["Name"]
        .values
    )


def test_trim(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    assert all_equal(trace.trim().events, trace.events)
    assert all_equal(
        trace.trim(0, 1).events, trace.events[trace.events["Timestamp (ns)"] == 0]
    )

    filtered = trace.filter("Timestamp (ns)", "between", [1e5, 1e6]).events
    trimmed = trace.trim(1e5, 1e6).events

    assert all_equal(filtered.index, trimmed.index)

    assert trimmed["Timestamp (ns)"].min() >= 1e5
    assert trimmed["_matching_timestamp"].min() >= 1e5
    assert trimmed["Timestamp (ns)"].max() <= 1e6
    assert trimmed["_matching_timestamp"].max() <= 1e6
