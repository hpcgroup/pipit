#!/usr/bin/env python

import pipit as pp


if __name__ == "__main__":
    # Path to OTF2 traces
    dirname = "../../pipit/tests/data/ping-pong-projections/pingpong.prj"

    # Use pipit's ``from_projections`` API to read in the Projections traces.
    # The result is stored into pipit's Trace data structure.
    trace = pp.Trace.from_projections(dirname)

    # Printout the DataFrame component of the Trace.
    print(trace.events)
