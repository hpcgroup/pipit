from pipit import Trace


"""
Note:
change later once reader schema is formalized
"""


def test_events(otf2_dir):
    events_df = Trace.from_otf2(str(otf2_dir)).events

    # 108 total events in ping pong trace
    assert(len(events_df) == 108)

    # fix this
    assert(set(events_df["Event Type"]) ==  set(["Enter", "Leave", "ProgramBegin",
                                            "ProgramEnd", "MpiSend", "MpiRecv"]))

    # fix this
    assert(set(events_df["Name"]) == set(["N/A", "MPI_Send", "MPI_Recv", "MPI_Init",
                                          "MPI_Finalize"]))

    # 8 sends per rank, so 16 sends total -> 32 (including both enter and leave rows)
    assert(len(events_df.loc[events_df["Name"] == "MPI_Send"]) == 32)

    assert(len(set(events_df["Location ID"])) == 2) # 2 ranks for ping pong

    assert(len(events_df.loc[events_df["Location ID"] == 0]) == 54) # 54 events per rank


def test_definitions(otf2_dir):
    definitions_df = Trace.from_otf2(str(otf2_dir)).definitions

    assert(len(definitions_df) == 229)

    # 17 total definitions in ping pong trace
    assert(len(set(definitions_df["Definition Type"]) == 17))

    # 2 ranks, so 2 definition locations in the trace
    assert(len(definitions_df.loc[definitions_df["Definition Type"] == "Location"] == 2))

    # communicator should evidently be present in the ping pong trace
    assert("Comm" in set(definitions_df["Definition Type"]))