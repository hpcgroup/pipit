from pipit import Trace


"""
Note:
change later once reader schema is formalized
"""


def test_events(otf2_dir):
    events_df = Trace.from_otf2(str(otf2_dir)).events

    assert(len(events_df) == 108)

    assert(set(events_df["Event Type"]) ==  set(["Enter", "Leave", "ProgramBegin",
                                            "ProgramEnd", "MpiSend", "MpiRecv"]))

    assert(set(events_df["Name"]) == set(["N/A", "MPI_Send", "MPI_Recv", "MPI_Init",
                                          "MPI_Finalize"]))

    assert(len(events_df.loc[events_df["Name"] == "MPI_Send"]) == len(events_df.loc[events_df["Name"] == "MPI_Send"]) == 32)

    assert(len(set(events_df["Location ID"])) == 2)

    assert(len(events_df.loc[events_df["Location ID"] == 0]) == 54)


def test_definitions(otf2_dir):
    definitions_df = Trace.from_otf2(str(otf2_dir)).definitions

    assert(len(definitions_df) == 229)

    assert(len(set(definitions_df["Definition Type"]) == 17))

    assert(len(definitions_df.loc[definitions_df["Definition Type"] == "Location"] == 2))

    assert("Comm" in set(definitions_df["Definition Type"]))


"""
def test_cct(otf2_dir):
    cct = Trace.from_otf2(str(otf2_dir)).cct

    # test cct
"""