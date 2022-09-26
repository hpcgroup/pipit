#!/usr/bin/env python

import pipit as pp


if __name__ == "__main__":
    # Path to HPCToolkit traces
    dirname = "../../pipit/tests/data/ping-pong-hpctoolkit"

    # Use pipit's ``from_hpctoolkit`` API to read in the traces.
    # The result is stored into pipit's Trace data structure.
    trace = pp.Trace.from_hpctoolkit(dirname)

    # Printout the DataFrame component of the Trace.
    print(trace.events)
