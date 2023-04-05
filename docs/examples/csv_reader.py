#!/usr/bin/env python

import pipit as pp


if __name__ == "__main__":
    # Use pipit's ``from_otf2`` API to read in the OTF2 traces.
    # The result is stored into pipit's Trace data structure.
    trace = pp.Trace.from_csv("../../pipit/tests/data/foo-bar.csv")

    trace.calc_inc_metrics()
    print(trace.events)
