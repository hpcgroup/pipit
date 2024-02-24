# Pipit Domain-Specific Language (DSL)

Pipit offers a robust lower-level API known as the **Pipit Domain-Specific Language (DSL)**, which contains primitives and functions for directly manipulating trace data from parallel programs.

The Pipit DSL can be used to create custom analysis tools and scripts. Pipit's high-level analysis API, visualization capabilities, and I/O operations are built on top of the Pipit DSL.

Furthermore, the Pipit DSL is implemented across a variety of backends, including Pandas and Polars, letting users choose the backend best suited for their use case. We are currently working on adding new backends, including distributed engines like Spark, to make Pipit highly scalable.

## Overview

In a parallel program, there are multiple units of execution, like MPI ranks. Many tracing tools, like Score-P, store the traces for each rank separately. Therefore, reading these traces and performing many operations are data-parallel workloads.

In the Pipit DSL, each rank's trace is contained in a `_Trace` object. All of the traces across all of the ranks are contained in a `TraceDataset` object, which represents the trace dataset of a single execution of a parallel program.

The `TraceDataset` object contains a dictionary mapping each rank to its `_Trace` object. It also contains functions for indexing, filtering, and manipulating events in the dataset. When one of these functions is called, the `TraceDataset` defers most of the computation to each `_Trace`, and simply aggregates the results returned by each `_Trace` into a well-formed response.

For instance, the `TraceDataset.filter` function looks something like this:

```python
def filter(self, condition):
    # Run the "filter" operation on each trace
    # filtered_traces is a dictionary of rank -> filtered trace
    filtered_traces = self.map_traces(lambda trace: trace.filter(condition))

    # Wrap filtered_traces in a new TraceDataset object
    return TraceDataset(filtered_traces)
```