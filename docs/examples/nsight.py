#!/usr/bin/env python

import pipit as pp


if __name__ == "__main__":
    # Path to Nsight traces
    filename = "../../pipit/tests/data/nbody-nvtx/trace.csv"

    # Use pipit's ``from_nsight`` API to read in the traces.
    # The result is stored into pipit's Trace data structure.
    trace = pp.Trace.from_nsight(filename)

    # Printout the DataFrame component of the Trace.
    print(trace.events)
