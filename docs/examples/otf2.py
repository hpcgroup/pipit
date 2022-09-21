#!/usr/bin/env python

import pipit as pp


if __name__ == "__main__":
    # Path to OTF2 traces
    dirname = "../../pipit/tests/data/ping-pong-otf2"

    # Use pipit's ``from_otf2`` API to read in the OTF2 traces.
    # The result is stored into pipit's Trace data structure.
    trace = pp.Trace.from_otf2(dirname)

    # Printout the DataFrame component of the Trace.
    print(trace.events)
