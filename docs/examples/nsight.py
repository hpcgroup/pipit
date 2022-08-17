#!/usr/bin/env python

import pipit as pp

# import pipit.ftc as ftc

if __name__ == "__main__":
    func = [
        "foo",
        "bar",
        "baz",
        "grault",
        "qux",
        "waldo",
        "bozo",
        "gerald",
        "boo",
        "new",
        "sheesh",
        "box",
        "tomato",
        "apple",
        "grapes",
        "orange",
    ]
    # ftc.FakeCreator(func).create_trace()

    # Path to NSight traces
    dirname = "../../pipit/tests/data/nbody-nvtx/fake_trace.csv"

    # Use pipit's ``from_nsight`` API to read in the traces.
    # The result is stored into pipit's Trace data structure.
    trace = pp.Trace.from_nsight(dirname)

    # Printout the DataFrame component of the Trace.
    print(trace.events)

    # trace.time_profile(time_bins=50)
