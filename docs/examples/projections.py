#!/usr/bin/env python

import pipit as pp
import time

if __name__ == "__main__":
    # Path to OTF2 traces
    # dirname = "../../pipit/tests/data/ping-pong-projections"
    dirname = "/Users/movsesyanae/Downloads/loimos-proj-coc-64"

    # Use pipit's ``from_projections`` API to read in the Projections traces.
    # The result is stored into pipit's Trace data structure.
    # time the trace creation
    for i in [1, 2, 4, 8, 16, 32, 64]:
        print("Number of processes: ", i)
        start_time = time.time()
        trace = pp.Trace.from_projections(dirname, num_processes=i)
        end_time = time.time()
        print("\tCore reader: ", end_time - start_time)

        start_time = time.time()
        trace = pp.Trace.from_projections_old(dirname, num_processes=i)
        trace._match_caller_callee()
        end_time = time.time()
        print("\tOld reader: ", end_time - start_time)

    # start_time = time.time()
    # trace._match_events()
    # trace._match_caller_callee()
    # end_time = time.time()
    # print("Time to match events and caller-callee: ", end_time - start_time)


    # Printout the DataFrame component of the Trace.
    # print(trace.events)
