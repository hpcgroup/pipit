.. Copyright 2022-2023 Parallel Software and Systems Group, University of
   Maryland. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

**********
User Guide
**********

Data Structures in Pipit
=============

Reading in a Dataset
=============

Trace Operations
=============

Extracting Calling Relationships
------------------

Raw traces are organized in the form of enter, leave or instant events and
their timestamps. We need to traverse and manipulate the DataFrame in the Trace
object to match rows that represent the start and end of a function or to
identify parent-child relationships using the nesting of events.  These
functions are necessary in order to start making sense of trace data in terms
of user functions and their calling contexts.  These functions are described
below.

_match_events: 

_match_caller_callee:

_create_cct:





Analyzing Overall Performance
------------------

Below is a list of descriptions of API functions that help analyze the time spent in different parts of the code.

calc_inc_metrics:

calc_exc_metrics:

flat_profile:

time_profile:

Analyzing Communication Performance
------------------

Below is a list of descriptions of API functions that help analyze communication patterns.

comm_matrix:

message_size_histogram:

comm_by_process:

comm_over_time:

Identifying Performance Issues
------------------

Here are some advanced operations that attempt to simplify the
identification of performance issues.

load_imbalance: 

idle_time:

outlier_detection:

pattern_detection:

multi_run_analysis:

Data Reduction
------------------

Pipit also supports filtering the DataFrame by different parameters to reduce the amount of data to analyze at a time. A user might be interested in analyzing the traces for a subset of processes or for a time period smaller than the entire execution.

filter:

Visualizing the Data
=============
