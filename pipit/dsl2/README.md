# Pipit Domain-Specific Language (DSL)

Pipit provides a lower-level API called the **Pipit Domain-Specific Language (DSL)**, which has primitives and functions for direct manipulation of trace data from parallel programs.

The Pipit DSL can be used to create custom analysis tools and scripts. Pipit's high-level analysis API, visualization capabilities, and I/O operations are built on top of the Pipit DSL.

Furthermore, the Pipit DSL is implemented across a variety of backends, including Pandas and Polars, letting users choose the backend that best fits their use case. We are currently working on adding new backends, including distributed engines like Spark, to make Pipit highly scalable.

## Overview

In a parallel program, there are multiple units of execution, like MPI ranks. Many tracing tools, like Score-P, store the traces for each rank separately. Therefore, reading these traces and performing many operations are data-parallel workloads.

In the Pipit DSL, each rank's trace is contained in a `_Trace` object. All traces across all ranks are consolidated in a `TraceDataset` object, representing the trace dataset from a single execution of a parallel program.

The `TraceDataset` object contains a dictionary mapping each rank to its corresponding `_Trace` object. It also provides functions for indexing, filtering, and manipulating events within the dataset. These functions map computations to each `_Trace`, and reduce the results returned by each `_Trace` into a well-formed response.

For instance, the `TraceDataset.filter` function looks like this:

```python
def filter(self, condition):
    # run the "filter" operation on each trace
    # filtered_traces is a dictionary of rank -> filtered _Trace
    filtered_traces = self.map_traces(lambda trace: trace.filter(condition))

    # wrap filtered_traces in a new TraceDataset object
    return TraceDataset(filtered_traces)
```

## Future work (potential)

The current Pipit DSL exploits a basic level of parallelism for MPI traces. Given the diverse nature of traces (across different tracing tools, programming models, hardware, etc.) the DSL can be extended to exploit additional parallelism, or to be generalized to support other traces.

However, increasing parallelism may cause too much overhead in certain cases, while increasing generality could complicate the DSL for both users and developers. Nonetheless, it is still worth exploring the below ideas to understand the tradeoffs:

1. Generalizing the concept of "rank" to accommodate any type of parallel event stream. For instance, in OpenMP traces, threads are the unit of parallelism instead of ranks.

1. Some tracing tools output one global trace file instead of one trace per stream:

    1. If possible, try and read the one file into multiple `_Trace` instances in parallel.
    1. If not, we can read the trace file serially, but then shuffle the data to different `_Trace` instances so that they can be operated on in parallel using `map_traces`.

    In either case, the idea is to support potentially multiple `_Trace` instances per trace file, so that parallelism isn't limited to the number of trace files.

1. Maximizing parallelism for hierarchical streams, such as multiple threads within each MPI rank, to achieve data parallelism across ranks and threads. Currently, `_Trace` represents a single stream. We can generalize `TraceDataset` to be a collection of either `_Trace` objects, or of other `TraceDataset` objects. Then, compute would be propagated all the way down to the `_Trace` level, and results would be aggregated up the hierarchy.

1. Abstracting the `TraceDataset` class to enable backend-specific implementations. Currently, the `_Trace` class is backend-specific. When abstracting `TraceDataset`, we would still have to keep an abstract `_Trace` class for operations like `map_traces` (since the user-provided function operates on an abstract trace, not a backend-specific object like a Pandas DataFrame).

    - While developing the Pipit DSL, I started out abstracting the `TraceDataset` class instead of the `_Trace` class, which led to difficulties implementing `map_traces`. To simplify the project, `TraceDataset` is currently a concrete class, mapping each rank to a `_Trace`. But this doesn't have to be the case. For instance, with a polars backend, we can get away with just using one big dataframe instead of a dataframe per trace. This is only possible by also abstracting `TraceDataset`, and implementing it specifically for polars.