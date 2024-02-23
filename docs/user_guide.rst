.. Copyright 2022-2023 Parallel Software and Systems Group, University of
   Maryland. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

**********
User Guide
**********

This guide is designed to help you get started with Pipit as quickly as possible. 
While Pipit can be used in a Python shell or file, we highly recommend using it within a Jupyter notebook for the best experience.

Assuming that you have installed Pipit, you can import it into your Python environment:

.. code-block:: python

  import pipit as pp

Data Structures in Pipit
========================

The primary data structure in Pipit is the ``Trace`` object, which corresponds to
a trace dataset on disk. The ``Trace`` object contains useful attributes, as
well as functions, to facilitate analysis of the underlying data. An important
attribute in the ``Trace`` object is the ``events`` DataFrame, which contains
the event data in a tabular format.

Reading in a Dataset
====================

We can read an OTF2 trace dataset into Pipit's ``Trace`` object as such:

.. code-block:: python

  trace = pp.Trace.from_otf2("pipit/pipit/tests/data/ping-pong-otf2")

Let's examine how Pipit has parsed events in the ``ping-pong-otf2`` dataset:

.. code-block:: python

  trace.events

.. image:: images/ping_pong_otf2.png
   :width: 600

The trace events are parsed into a Pandas DataFrame, where each row represents an event
and each column represents an attribute of that event.

Understanding the DataFrame Columns
================

The ``events`` DataFrame includes the following columns:

- **Timestamp (ns)**: The timestamp of the event in nanoseconds.
- **Event Type**: The type of the event. This can be one of three values:

  - **Enter**: Indicates the start of a region of code (usually a function invocation).
  - **Leave**: Indicates the end of a region of code (usually a function return).
  - **Instant**: This event is instantaneous, typically an MPI communication event (like "send" or "receive").
  
- **Name**: A descriptive identifier for the event. For Enter and Leave events, this is usually the name of the function.
- **Thread**: The thread in which the event occurred.
- **Process**: The process in which the event occurred. For MPI programs, this represents the MPI rank.
- **Attributes**: Additional information or metadata about the event. For Instant events associated with MPI communication, this may contain the size of the message being sent or received.

Pipit supports a variety of trace formats, including HPCToolkit, OTF2, Nsight, and Projections.
You can also write a custom reader to parse event data into the above columns, allowing Pipit
to accommodate any format.


Trace Operations
================

Extracting Calling Relationships
--------------------------------

Suppose we want to determine how long a function call takes. A function call involves two events: the Enter event marks the start of the
function's execution, and the Leave event indicates when the function completes and returns.
Therefore, it is useful to match these two events with each other:

.. code-block:: python

  trace._match_events()
  trace.events

.. image:: images/_match_events.png
   :width: 700

Now, we have two new columns in the DataFrame, **_matching_event**
and **_matching_timestamp**, which represent the index and timestamp of the
corresponding Leave event (for Enter events), and the corresponding Enter event
(for Leave events).

This simplifies the calculation of the time spent for each function call:

.. math::

   \text{time spent} = \left| \text{matching\_timestamp} - \text{Timestamp (ns)} \right|

Suppose instead, we want to calculate the *exclusive* time spent in a function call
(also called "self" time)? That is, we would like to subtract the time spent in 
all nested (child) function calls:

.. math::

   \text{exclusive time} = \text{time spent} - \sum \text{time spent in children}

For this, we need to know the child functions associated with 
each function call:

.. code-block:: python

  trace._match_caller_callee()
  trace.events

.. image:: images/_match_caller_callee.png
   :width: 700

So far, we've added several columns to the DataFrame: _matching_event,
_matching_timestamp, _depth, _parent, and _children. As you'll see, Pipit lets users add as many columns
as necessary to faciliate analysis. Having such intermediate results makes it easier to perform
further calculations.

Analyzing Overall Performance
-----------------------------

Now that we have read in trace data and completed some essential preprocessing on the events,
we are ready to perform analysis.


.. code-block:: python

  trace.calc_inc_metrics()
  trace.events
  

.. image:: images/calc_inc_metrics.png
   :width: 700

We add yet another column to the DataFrame: **time.inc**. As discussed above, this column contains
the *inclusive* (or total) time spent on a particular function call. While a function call
is represented by an Enter row and a Leave row, we only store this value on the Enter row to avoid
taking up extra space. Instead of manually doing this calculation, Pipit has done it for us.

.. note::
   Pipit computes the inclusive time using the same formula mentioned above,
   with some added bells and whistles (like handling edge cases). In addition,
   ``calc_inc_metrics`` calculates not only the inclusive time, but also
   other inclusive metrics that may be present in the trace, like values of hardware counters.

Similarly, we can compute the *exclusive* (also known as "self") time spent on each function call:

.. code-block:: python

  trace.calc_exc_metrics()
  trace.events
  
.. image:: images/calc_exc_metrics.png
   :width: 700

Again, our DataFrame is populated with a new column, **time.exc**, representing this value. Pipit
has subtracted the times taken by all children functions to calculate the exclusive time. In fact,
Pipit uses the _children column calculated previously to do so.

.. note::
   If you are familiar with Pandas, try doing ``trace.events.sort_values("time.exc", ascending=False)``.
   This will return a copy of the ``events`` DataFrame, sorted from the longest function execution
   to the shortest, letting you quickly see which function calls are taking the most time.

Finally, let's take a look at the trace's *flat profile*. A flat profile aggregates metrics, typically by function name.
For instance, we can easily see the *total* inclusive and exclusive times for each distinct function:

.. code-block:: python

  trace.flat_profile()

.. image:: images/flat_profile.png
   :width: 200

We can also break this information down on a per-process basis:

.. code-block:: python

  trace.flat_profile(per_process=True)


.. image:: images/flat_profile_per_process.png
   :width: 230


.. note::
   Notice how in the past two examples, we don't invoke ``trace.events`` at the end. This is because
   the ``flat_profile`` function returns a DataFrame containing the flat profile. In contrast,
   functions like  ``calc_inc_metrics`` and ``calc_exc_metrics`` don't return anything; they perform
   computations and store the results in the ``events`` DataFrame.


Analyzing Communication Performance
-----------------------------------

While Pipit offers numerous functions for analyzing compute performance,
another significant bottleneck in HPC applications is communication performance. Pipit provides
useful API functions to help us understand how communication bottlenecks might impact our application.

The *communication matrix* of an application is a 2D array representing
the volume of data exchanged between pairs of processes. If MPI communication data is present
in the trace, Pipit can compute and output the communication matrix:

.. code-block:: python

  trace.comm_matrix()

.. image:: images/comm_matrix.png
   :width: 200

In this matrix, the first dimension corresponds to the sending process, and the second dimension
represents the receiving process. We can determine the number of bytes process *a* sends to process *b*
with the following syntax:

.. code-block:: python

  cm = trace.comm_matrix()
  cm[a][b]

Additionally, it may be helpful to find the total number of bytes sent and received by
each process to identify communication imbalances. 

.. code-block:: python

  trace.comm_by_process()


.. image:: images/comm_by_process.png
   :width: 210


.. Identifying Performance Issues
.. ------------------------------



.. Here are some advanced operations that attempt to simplify the
.. identification of performance issues.

.. **load_imbalance:**

.. **idle_time:**

.. **outlier_detection:**

.. **pattern_detection:**

.. **multi_run_analysis:**

.. Data Reduction
.. --------------

.. Pipit also supports filtering the DataFrame by different parameters to reduce the amount of data to analyze at a time. A user might be interested in analyzing the traces for a subset of processes or for a time period smaller than the entire execution.

.. **filter:**

Visualizing the Data
====================

While Pipit is mainly designed for programmatic analysis, it also includes a simple
visual interface to complement the API functions.

A common and straightforward visualization for event traces is a timeline, also known as a Gantt chart.
In this view, events are laid out in chronological order:

.. code-block:: python

  trace.plot_timeline()

.. image:: images/plot_timeline.png
   :width: 700

In addition, Pipit provides visualization support for the results of many of its
analysis functions. The visualization API closely mirrors the programmatic API:

.. code-block:: python

  trace.plot_flat_profile()

.. code-block:: python

  trace.plot_comm_matrix()

.. warning::
   The visual interface is still a work-in-progress, and can be tested out
   in the ``vis`` branch.

Summary
=======

In this guide, you have learned the basics of the Pipit library. The workflow can be summarized as follows:

1. Import the Pipit library.
2. Read a trace dataset into a ``Trace`` instance.
3. Use ``Trace`` instance methods to perform calculations and analysis as necessary.

.. note::
   For advanced Python users, especially those familiar with Pandas and NumPy, Pipit 
   exposes the ``events`` DataFrame for custom analysis.

This guide serves to privde a gentle introduction to Pipit without providing an exhaustive list of its API
functions and features. We highly recommend exploring example notebooks and referring to the Pipit API documentation
for a comprehensive understanding of the library's capabilities.

Pipit can be used to perform automated analysis through Python scripts, as well 
as exploratory analysis via Jupyter notebooks. Since performance bottlenecks
are not typically not known ahead of time, we highly recommend using Pipit within a Jupyter notebook.
This allows for interactive exploration of performance data, facilitating a deeper understanding and providing valuable insights.