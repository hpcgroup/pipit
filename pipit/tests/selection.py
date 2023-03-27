from pipit import Trace


def all_equal(*dfs):
    return all([dfs[0].equals(df) for df in dfs])


def test_loc(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    # index
    assert all_equal(trace.loc[12], trace.events.loc[12])

    # slice
    assert all_equal(trace.loc[30:45].events, trace.events.loc[30:45])

    # boolean array
    arr = [False] * len(trace.events)
    arr[5] = True
    arr[16] = True
    arr[23] = True
    arr[78] = True
    arr[107] = True

    assert all_equal(trace.loc[arr].events, trace.events.loc[arr])


def test_eval(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    # Test each operator
    assert all_equal(
        trace.eval("Process", "==", 0),
        trace.eval(expr="`Process` == 0"),
        trace.events["Process"] == 0,
    )
    assert all_equal(
        trace.eval("Name", "!=", "MPI_Init"),
        trace.eval(expr="`Name` != 'MPI_Init'"),
        trace.events["Name"] != "MPI_Init",
    )
    assert all_equal(
        trace.eval("Timestamp (ns)", "<", "130.52 ms"),
        trace.eval("Timestamp (ns)", "<", 1.3052e8),
        trace.eval(expr="`Timestamp (ns)` < 1.3052e8"),
        trace.events["Timestamp (ns)"] < 1.3052e8,
    )
    assert all_equal(
        trace.eval("Timestamp (ns)", "<=", 130504301.50129148),
        trace.eval(expr="`Timestamp (ns)` <= 130504301.50129148"),
        trace.events["Timestamp (ns)"] <= 130504301.50129148,
    )
    assert all_equal(
        trace.eval("Timestamp (ns)", ">", "130.52 ms"),
        trace.eval("Timestamp (ns)", ">", 1.3052e8),
        trace.eval(expr="`Timestamp (ns)` > 1.3052e8"),
        trace.events["Timestamp (ns)"] > 1.3052e8,
    )
    assert all_equal(
        trace.eval("Timestamp (ns)", ">=", 130532982.41000055),
        trace.eval(expr="`Timestamp (ns)` >= 130532982.41000055"),
        trace.events["Timestamp (ns)"] >= 130532982.41000055,
    )
    assert all_equal(
        trace.eval("Name", "in", ["MPI_Send", "MPI_Recv"]),
        trace.eval(expr="`Name`.isin(['MPI_Send', 'MPI_Recv'])"),
        trace.events["Name"].isin(["MPI_Send", "MPI_Recv"]),
    )
    assert all_equal(
        trace.eval("Name", "not-in", ["MPI_Send", "MPI_Recv"]),
        trace.eval(expr="~(`Name`.isin(['MPI_Send', 'MPI_Recv']))"),
        ~trace.events["Name"].isin(["MPI_Send", "MPI_Recv"]),
    )
    assert all_equal(
        trace.eval("Timestamp (ns)", "between", ["16 ms", "130.505 ms"]),
        (trace.events["Timestamp (ns)"] > 1.6e5)
        & (trace.events["Timestamp (ns)"] < 1.30505e8),
    )

    # Test logical operators NOT, AND, and OR
    from pipit.selection import BooleanExpr

    e1 = BooleanExpr("Timestamp (ns)", "between", ["130.52 ms", "136.57 ms"])
    e2 = BooleanExpr("Name", "in", ["MPI_Send", "MPI_Recv"])
    e3 = BooleanExpr("Process", "==", 0)

    assert all_equal(trace.eval(~e3), ~trace.eval(e3))

    assert all_equal(
        trace.eval(e1 & e2 & e3),
        trace.eval(e1) & trace.eval(e2) & trace.eval(e3),
    )
    assert all_equal(
        trace.eval(e1 | e2 | e3),
        trace.eval(e1) | trace.eval(e2) | trace.eval(e3),
    )

    # Test validate
    keep = trace.eval("Timestamp (ns)", ">", "133 ms", validate="keep")
    invalid = trace.eval("Timestamp (ns)", ">", "133 ms", validate=False)

    assert all_equal(invalid, trace.events["Timestamp (ns)"] > 1.33e08)

    assert invalid.sum() < keep.sum()


def test_query(data_dir, ping_pong_otf2_trace):
    trace = Trace.from_otf2(str(ping_pong_otf2_trace))

    assert all_equal(
        trace.query("Process", "==", 0).definitions,
        trace.loc[trace.eval("Process", "==", 0)].definitions,
        trace.definitions,
    )

    assert all_equal(
        trace.query("Process", "==", 0).events,
        trace.loc[trace.eval("Process", "==", 0)].events,
        trace.events[trace.events["Process"] == 0],
    )

    # Test validate
    keep = trace.query("Timestamp (ns)", ">", "133 ms", validate="keep").events
    invalid = trace.query("Timestamp (ns)", ">", "133 ms", validate=False).events

    assert len(keep[keep["Event Type"] == "Enter"]) == len(
        keep[keep["Event Type"] == "Leave"]
    )
    assert len(invalid[invalid["Event Type"] == "Enter"]) != len(
        invalid[invalid["Event Type"] == "Leave"]
    )
