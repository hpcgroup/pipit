# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT
import numpy
import numpy as np
import pandas as pd

from pipit.util.cct import create_cct
import narwhals as nw
import narwhals.selectors as ncs
from narwhals.typing import FrameT
import math as math


class Trace:
    """
    A trace dataset is read into an object of this type, which
    includes one or more dataframes and a calling context tree.
    """

    def __init__(self, definitions, events: FrameT, cct=None):
        """Create a new Trace object."""
        self.definitions = definitions
        self.events: FrameT = events
        self.cct = cct

        # list of numeric columns which we can calculate inc/exc metrics with
        self.numeric_cols = self.events.select(ncs.numeric()).columns
        self.numeric_cols.remove("unique_id")
        self.numeric_cols.remove("_parent")
        self.numeric_cols.remove("_matching_event")
        self.numeric_cols.remove("_matching_timestamp")

        # Native Namespace is the backend
        self.native_namespace = nw.get_native_namespace(self.events)

        # will store columns names for inc/exc metrics
        self.inc_metrics = []
        self.exc_metrics = []

    def create_cct(self):
        # adds a column of cct nodes to the events dataframe
        # and stores the graph object in self.cct
        self.cct = create_cct(self.events)

    @staticmethod
    def from_otf2(dirname, num_processes=None, frame_backend=pd.DataFrame, create_cct=False):
        """Read an OTF2 trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.otf2_reader import OTF2Reader

        return OTF2Reader(dirname, num_processes, frame_backend, create_cct).read()

    @staticmethod
    def from_hpctoolkit(dirname):
        """Read an HPCToolkit trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.hpctoolkit_reader import HPCToolkitReader

        return HPCToolkitReader(dirname).read()

    @staticmethod
    def from_projections(dirname, num_processes=None, create_cct=False):
        """Read a Projections trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.projections_reader import ProjectionsReader

        return ProjectionsReader(dirname, num_processes, create_cct).read()

    @staticmethod
    def from_nsight(filename, create_cct=False):
        """Read an Nsight trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.nsight_reader import NsightReader

        return NsightReader(filename, create_cct).read()

    @staticmethod
    def from_csv(filename):
        events_dataframe = pd.read_csv(filename, skipinitialspace=True)

        # if timestamps are in seconds, convert them to nanoseconds
        if "Timestamp (s)" in events_dataframe.columns:
            events_dataframe["Timestamp (s)"] *= 10**9
            events_dataframe.rename(
                columns={"Timestamp (s)": "Timestamp (ns)"}, inplace=True
            )

        # ensure that ranks are ints
        events_dataframe = events_dataframe.astype({"Process": "int32"})

        # make certain columns categorical
        events_dataframe = events_dataframe.astype(
            {
                "Event Type": "category",
                "Name": "category",
                "Process": "category",
            }
        )

        # sort the dataframe by Timestamp
        events_dataframe.sort_values(
            by="Timestamp (ns)", axis=0, ascending=True, inplace=True, ignore_index=True
        )

        return Trace(None, events_dataframe)

    def to_chrome(self, filename=None):
        """Export as Chrome Tracing JSON, which can be opened
        in Perfetto."""
        from .writers.chrome_writer import ChromeWriter

        return ChromeWriter(self, filename).write()

    def _match_events(self):
        """Matches corresponding enter/leave events and adds two columns to the
        dataframe: _matching_event and _matching_timestamp
        """

        if "_matching_event" not in self.events.columns:
            max_unique_id = self.events['unique_id'].max()
            matching_events = [-1] * len(range(max_unique_id))
            matching_times = [-1] * len(range(max_unique_id))

            # only pairing enter and leave rows
            enter_leave_df = self.events.filter(nw.col("Event Type").is_in(["Enter", "Leave"]))

            # list of processes and/or threads to iterate over
            if "Thread" in self.events.columns:
                exec_locations = set(zip(self.events["Process"], self.events["Thread"]))
                has_thread = True
            else:
                exec_locations = set(self.events["Process"])
                has_thread = False

            for curr_loc in exec_locations:
                # only filter by thread if the trace has a thread column
                if has_thread:
                    curr_process, curr_thread = curr_loc
                    filtered_df = enter_leave_df.filter(nw.col("Process") == curr_process).filter(nw.col("Thread") == curr_thread)
                else:
                    filtered_df = enter_leave_df.filter(nw.col("Process") == curr_loc)

                stack = []

                # Note: The reason that we are creating lists that are
                # copies of the dataframe columns below and iterating over
                # those instead of using pandas iterrows is due to an
                # observed improvement in performance when using lists.

                event_types = list(filtered_df["Event Type"])
                df_indices, timestamps, names = (
                    list(filtered_df["unique_id"]),
                    list(filtered_df["Timestamp (ns)"]),
                    list(filtered_df["Name"]),
                )

                # Iterate through all events of filtered DataFrame
                for i in range(len(filtered_df)):
                    curr_df_index, curr_timestamp, evt_type, curr_name = (
                        df_indices[i],
                        timestamps[i],
                        event_types[i],
                        names[i],
                    )

                    if evt_type == "Enter":
                        # Add current dataframe index and timestamp to stack
                        stack.append((curr_df_index, curr_timestamp, curr_name))
                    else:
                        # we want to iterate through the stack in reverse order
                        # until we find the corresponding "Enter" Event
                        enter_name, i = None, len(stack) - 1
                        while enter_name != curr_name and len(stack) > 0:
                            enter_df_index, enter_timestamp, enter_name = stack.pop()

                        if enter_name == curr_name:
                            # Fill in the lists with the matching values if event found
                            matching_events[enter_df_index] = curr_df_index
                            matching_events[curr_df_index] = enter_df_index

                            matching_times[enter_df_index] = curr_timestamp
                            matching_times[curr_df_index] = enter_timestamp
                        else:
                            continue

            # since narwhals doesn't guarantee the order of the rows,
            # we do a join on the unique_id column to ensure that the
            # matching events and timestamps are in the correct order
            self.events = self.events.join(
                nw.from_dict({
                    "_matching_event": matching_events,
                    "_matching_timestamp": matching_times,
                    "unique_id": list(range(max_unique_id))
                }, native_namespace=nw.get_native_namespace(self.events)),
                on="unique_id")


    def _match_caller_callee(self):
        """Matches callers (parents) to callees (children) and adds two
        columns to the dataframe:
        _depth, _parent
        _depth is the depth of the event in the call tree (starting from 0 for root)
        _parent is the dataframe index of a row's parent event.
        _children is a list of dataframe indices of a row's children events.
        """

        if "_parent" not in self.events.columns:
            max_unique_id = self.events['unique_id'].max()
            depth, parent = [-1] * max_unique_id, [-1] * max_unique_id

            # match events so we can
            # ignore unmatched ones
            self._match_events()

            # list of processes and/or threads to iterate over
            if "Thread" in self.events.columns:
                exec_locations = set(zip(self.events["Process"], self.events["Thread"]))
                has_thread = True
            else:
                exec_locations = set(self.events["Process"])
                has_thread = False

            for curr_loc in exec_locations:
                # only filter by thread if the trace has a thread column
                if has_thread:
                    curr_process, curr_thread = curr_loc
                    # filtered_df = enter_leave_df.loc[
                    #     (enter_leave_df["Process"] == curr_process)
                    #     & (enter_leave_df["Thread"] == curr_thread)
                    # ]
                    filtered_df = self.events.filter(nw.col("Process") == curr_process).filter(
                        nw.col("Thread") == curr_thread)
                else:
                    # filtered_df = enter_leave_df.loc[
                    #     (enter_leave_df["Process"] == curr_loc)
                    # ]
                    filtered_df = self.events.filter(nw.col("Process") == curr_loc)

                stack = []
                df_indices, event_types = list(filtered_df['unique_id']), list(
                    filtered_df["Event Type"]
                )

                # loop through the events of the filtered dataframe
                for i in range(len(filtered_df)):
                    curr_df_index, evt_type = df_indices[i], event_types[i]

                    if evt_type == "Enter" or evt_type == "Instant":
                        if len(stack) > 0:  # if event is a child of some other event
                            parent_df_index = stack[-1]

                            parent[curr_df_index] = parent_df_index

                        depth[curr_df_index] = len(stack)
                        if evt_type == "Enter":
                            # add enter dataframe index to stack
                            stack.append(curr_df_index)
                    else:
                        # pop event off stack once matching leave found
                        # Note: parent, and children for a leave row
                        # can be found using the matching index that
                        # corresponds to the enter row
                        stack.pop()

            self.events = self.events.join(nw.from_dict({
                "_depth": depth,
                "_parent": parent,
                "unique_id": list(range(max_unique_id))
            }, native_namespace=nw.get_native_namespace(self.events)), how='left', on="unique_id")


    def calc_inc_metrics(self, columns=None):
        # pair enter and leave rows
        if "_matching_event" not in self.events.columns:
            self._match_events()

        # if no columns are specified by the user, then we calculate
        # inclusive metrics for all the numeric columns in the trace
        columns = self.numeric_cols if columns is None else columns
        columns_to_compute = []

        # get the corresponding metric column name for columns provided,
        # ignoring the columns that have already been calculated
        tmp_leave_col_names = []
        for col_name in columns:
            metric_col_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            if metric_col_name not in self.events.columns:
                tmp_leave_col_names.append("leave_" + col_name)
                columns_to_compute.append(col_name)
        if len(columns_to_compute) == 0:
            return

        # Get only leave events
        leave_frame = self.events.filter(nw.col('Event Type') == 'Leave')
        # Select only the needed columns (metric columns and matching event)
        leave_frame = leave_frame.rename({col_name: "leave_" + col_name for col_name in columns_to_compute}).select(
            ["_matching_event"] + tmp_leave_col_names
        )

        # Join the leave events with the original events, creating a new column for each metric
        # populated with the value of the metric at the leave event
        self.events = self.events.join(leave_frame, left_on='unique_id', right_on='_matching_event',
                                       how="left")
        # Create list of expressions to calculate the inclusive metrics
        # Each expression is (metric_value_at_leave - metric_value_at_enter)
        exp_list = []
        for col_name in columns_to_compute:
            metric_col_inc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            exp_list.append((nw.col("leave_"+col_name) - nw.col(col_name)).alias(metric_col_inc_name))
        self.events = self.events.with_columns(exp_list).drop(tmp_leave_col_names)


    def calc_exc_metrics(self, columns=None):
        # calculate exc metrics for all numeric columns if not specified
        columns = self.numeric_cols if columns is None else columns


        # Filter out already calculated columns
        columns_to_compute = []
        # Create list of aggregations to do (each metric)
        exp_list = []
        # create list of new column names for the sum of inclusive metrics
        metric_col_inc_names = []
        for col_name in columns:
            metric_col_exc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".exc"
            metric_col_inc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            if metric_col_exc_name not in self.events.columns:
                columns_to_compute.append(col_name)
                exp_list.append(nw.col(metric_col_inc_name).sum().alias('child_' + metric_col_inc_name))
                metric_col_inc_names.append(metric_col_inc_name)
        if len(columns_to_compute) == 0:
            return


        # match caller and callee rows
        self._match_caller_callee()

        # calculate inclusive metrics if needed
        self.calc_inc_metrics(columns_to_compute)

        # get the enter events
        enter_frame = self.events.filter(nw.col('Event Type') == 'Enter')

        # group by the parent unique id and aggregate the sum of the inclusive metrics
        grouped_parents_sum_frame = (enter_frame.group_by('_parent').agg(exp_list)
                                     .select(['_parent'] + ['child_' + col_name for col_name in metric_col_inc_names])
                                     .filter(nw.col('_parent') != -1))

        # join with enter events, connecting each parent with the sum of the inclusive metrics of its children
        self.events = self.events.join(grouped_parents_sum_frame, left_on='unique_id', right_on='_parent', how='left')

        # make list of expressions to calculate the exclusive metrics (inclusive - sum of children)
        exp_list = []
        tmp_metric_col_inc_names = []
        for col_name in columns_to_compute:
            metric_col_inc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            metric_col_exc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".exc"
            exp_list.append((nw.col(metric_col_inc_name) - nw.col('child_' + metric_col_inc_name).
                             fill_null(0)).alias(metric_col_exc_name))
            tmp_metric_col_inc_names.append('child_' + metric_col_inc_name)

        self.events = self.events.with_columns(
            exp_list
        ).drop(tmp_metric_col_inc_names)

    def comm_matrix(self, output="size"):
        """
        Communication Matrix for Peer-to-Peer (P2P) MPI messages

        Arguments:
        1) output -
        string to choose whether the communication volume should be measured
        by bytes transferred between two processes or the number of messages
        sent (two choices - "size" or "count")

        Returns:
        Creates three lists - sender ranks, receiver ranks, and message volume.
        All of these lists are the length of the number of messages sent in the trace.
        It then loops through these lists containing individual message pairs
        and volume for those messages and updates the comm matrix.

        Finally, a 2D Numpy Array that represents the communication matrix for all P2P
        messages of the given trace is returned.

        Note:
        The first dimension of the returned 2d array
        is senders and the second dimension is receivers
        ex) comm_matrix[sender_rank][receiver_rank]
        """

        # get the list of ranks/processes
        # (mpi messages are sent between processes)
        ranks = set(self.events["Process"])

        # create a 2d numpy array that will be returned
        # at the end of the function
        communication_matrix = np.zeros(shape=(len(ranks), len(ranks)))

        # filter the dataframe by MPI Send and Isend events
        sender_frame = self.events.filter(
            nw.col("Name").is_in(["MpiSend", "MpiIsend"])
        )

        # get the mpi ranks of all the sender processes
        # the length of the list is the total number of messages sent
        sender_ranks = sender_frame["Process"].to_list()

        # get attributes dicts of all the sender processes
        attributes_list = sender_frame["Attributes"].to_list()

        # get the corresponding mpi ranks of the receivers
        # the length of the list is the total number of messages sent
        receiver_ranks = [attrDict["receiver"] for attrDict in attributes_list]

        # the length of the message_volume list created below
        # is the total number of messages sent

        # number of bytes communicated for each message sent
        if output == "size":
            # (1 communication is a single row in the sender dataframe)
            message_volume = [attrDict["msg_length"] for attrDict in attributes_list]

        elif output == "count":
            # 1 message between the pairs of processes
            # for each row in the sender dataframe
            message_volume = np.full(len(sender_frame), 1)

        for i in range(len(sender_ranks)):
            """
            loops through all the communication events and adds the
            message volume to the corresponding entry of the 2d array
            using the sender and receiver ranks
            """
            communication_matrix[sender_ranks[i], receiver_ranks[i]] += message_volume[
                i
            ]

        return communication_matrix

    def message_histogram(self, bins=20, **kwargs):
        """Generates histogram of message frequency by size."""

        # Get message attributes
        messages = list(self.events.filter(nw.col('Name').is_in(["MpiSend", "MpiIsend"]))['Attributes'])
        # Get message sizes
        sizes = [message['msg_length'] for message in messages]

        return np.histogram(sizes, bins=bins, **kwargs)

    def comm_over_time(self, output="size", message_type="send", bins=50, **kwargs):
        """Returns histogram of communication volume over time.

        Args:
            output (str, optional). Whether to calculate communication by "count" or
            "size". Defaults to "size".

            message_type (str, optional): Whether to compute for sends or
            receives. Defaults to "send".

            bins (int, optional): Number of bins in the histogram. Defaults to
            50.

        Returns:
            hist: Volume in size or number of messages in each time interval
            edges: Edges of time intervals
        """
        # Filter by send or receive events
        if message_type == "send":
            events = self.events.filter(nw.col('Name').is_in(["MpiSend", "MpiIsend"]))
        else:
            events = self.events.filter(nw.col('Name').is_in(["MpiRecv", "MpiIrecv"]))


        # Get timestamps and sizes
        timestamps = list(events["Timestamp (ns)"])
        attributes = list(events["Attributes"])
        sizes = [attr["msg_length"] for attr in attributes]

        return np.histogram(
            timestamps,
            bins=bins,
            weights=sizes if output == "size" else None,
            range=[
                self.events["Timestamp (ns)"].min(),
                self.events["Timestamp (ns)"].max(),
            ],
            **kwargs
        )

    def comm_by_process(self, output="size"):
        """Returns total communication volume in size or number of messages per
           process.

        Returns:
            pd.DataFrame: DataFrame containing total communication volume or
            number of messags sent and received by each process.
        """
        comm_matrix = self.comm_matrix(output=output)

        # Get total sent and received for each process
        sent = comm_matrix.sum(axis=1)
        received = comm_matrix.sum(axis=0)

        return pd.DataFrame({"Sent": sent, "Received": received}).rename_axis("Process")

    def flat_profile(
        self, metrics="time.exc", groupby_column="Name", per_process=False
    ):
        """
        Arguments:
        metrics - a string or list of strings containing the metrics to be aggregated
        groupby_column - a string or list containing the columns to be grouped by

        Returns:
        A Pandas DataFrame that will have the aggregated metrics
        for the grouped by columns.
        """

        metrics = [metrics] if not isinstance(metrics, list) else metrics

        # calculate inclusive time if needed
        if "time.inc" in metrics:
            self.calc_inc_metrics(["Timestamp (ns)"])

        # calculate exclusive time if needed
        if "time.exc" in metrics:
            self.calc_exc_metrics(["Timestamp (ns)"])

        # This first groups by both the process and the specified groupby
        # column (like name). It then sums up the metrics for each combination
        # of the process and the groupby column.
        per_process_flat_profile = (self.events.filter(nw.col("Event Type") == "Enter")
                                    .group_by([groupby_column, "Process"])
                                    .agg([nw.sum(metric) for metric in metrics]))
        if per_process:
            return per_process_flat_profile
        else:
            return per_process_flat_profile.group_by(groupby_column).agg([nw.mean(metric) for metric in metrics])

    def load_imbalance(self, metric="time.exc", num_processes=1):
        """
        Arguments:
        metric - a string denoting the metric to calculate load imbalance for
        num_processes - the number of ranks to display for each function that have the
        highest load imbalances

        Returns:
        A Pandas DataFrame indexed by function name that will have two columns:
        one containing the imabalance which (max / mean) time for all ranks
        and the other containing a list of num_processes ranks with the highest
        imbalances
        """

        num_ranks = len(set(self.events["Process"]))
        num_display = num_ranks if num_processes > num_ranks else num_processes

        flat_profile = (self.flat_profile(metrics=metric, per_process=True)
                        .sort(by=metric, descending=True, nulls_last=True))

        imbalance_dict = dict()

        imb_metric = metric + ".imbalance"
        imb_ranks = "Top processes"
        mean_metric = metric + ".mean"

        imbalance_dict[imb_metric] = []
        imbalance_dict[imb_ranks] = []
        imbalance_dict[mean_metric] = []

        functions = list(self.events.filter(nw.col("Event Type") == "Enter")["Name"].unique())

        for function in functions:

            # Already sorted from most to least of the metric
            filtered_fp = flat_profile.filter(nw.col('Name') == function)

            top_metric_value = filtered_fp[metric][0]

            top_n_ranks = list(filtered_fp["Process"][0:num_display])

            function_avg = filtered_fp[metric].mean()

            imbalance_dict[mean_metric].append(function_avg)
            imbalance_dict[imb_metric].append(top_metric_value / function_avg)
            imbalance_dict[imb_ranks].append(top_n_ranks)

        imbalance_dict['Name'] = functions

        imbalance_frame = nw.from_dict(imbalance_dict, native_namespace=self.native_namespace)

        return imbalance_frame

    def idle_time(self, idle_functions=["Idle"], mpi_events=False):

        # Calculate inclusive time metric if not present
        if "time.inc" not in self.events.columns:
            self.calc_inc_metrics()

        # Update idle functions if mpi_events is True
        if mpi_events:
            idle_functions += ["MPI_Wait", "MPI_Waitall", "MPI_Recv"]

        # Filter for Enter row of "idle" functions
        filtered_frame = self.events.filter([
            nw.col("Event Type") == "Enter",
            nw.col("Name").is_in(idle_functions)
        ])

        # Group by process and sum the inclusive time
        return filtered_frame.group_by("Process").agg(nw.sum("time.inc"))


    def time_profile(self, num_bins=50, normalized=False):
        """Computes time contributed by each function per time interval.

        Args:
            num_bins (int, optional): Number of evenly-sized time intervals to compute
                time profile for. Defaults to 50.
            normalized (bool, optional): Whether to return time contribution as
                percentage of time interval. Defaults to False.

        Returns:
            pd.DataFrame: Time profile of each function, where each column
                represents a function, and each row represents a time interval.
        """
        # Generate metrics
        self._match_caller_callee()
        self.calc_inc_metrics(["Timestamp (ns)"])

        # Filter by Enter rows
        events = self.events.filter(nw.col('Event Type') == 'Enter')

        # Create equal-sized bins
        edges = np.linspace(
            self.events["Timestamp (ns)"].min(),
            self.events["Timestamp (ns)"].max(),
            num_bins + 1,
        )
        bin_size = edges[1] - edges[0]

        total_bin_duration = bin_size * len(events["Process"].unique())

        profile = []

        def calc_exc_time_in_bin(events_frame: FrameT):
            # Assume events_frame is only the enter events that belong in the bin

            # Group by the parent and sum the inclusive time
            # Filter out the -1 parent (root)
            grouped_parents_sum_frame = (events_frame.group_by('_parent')
                                         .agg(nw.sum('inc_time_in_bin').alias('child_inc_time_in_bin'))
                                         .select(['_parent', 'child_inc_time_in_bin'])
                                         .filter(nw.col('_parent') != -1))

            # join with the grouped parents sum frame
            # connecting each parent with the sum of the inclusive metrics of its children
            events_frame = events_frame.join(grouped_parents_sum_frame, left_on='unique_id',
                                             right_on='_parent', how='left')
            # Fill in the null values (i.e. no children) with 0
            events_frame = events_frame.with_columns(nw.col('child_inc_time_in_bin').fill_null(0))
            # Calculate the exclusive time in the bin and select only the necessary columns
            events_frame = events_frame.with_columns((nw.col('inc_time_in_bin') - nw.col('child_inc_time_in_bin'))
                                                     .alias('exc_time_in_bin')).select(['Name', 'exc_time_in_bin'])
            return events_frame

        # For each bin, determine each function's time contribution
        for i in range(num_bins):
            start = edges[i]
            end = edges[i + 1]

            # Find functions that belong in this bin
            in_bin_frame: FrameT = events.filter(
                (nw.col('_matching_timestamp') > start) & (nw.col('Timestamp (ns)') < end)
            )

            # We have 4 cases to how a function is in a bin
            # ---------------------------------------------
            # Case 1: (>= start) & (> end)
            #   (Function starts in bin and extends)
            # Case 2: (< start) & (<= end)
            #   (Function ends in bin and started before)
            # Case 3: (< start) & (> end)
            #   (Function spans bin)
            # Case 4: (>= start) & (<= end)
            #   (Function contained in bin)

            # Now we convert these case to nested conditionals
            # ------------------------------------------------
            # If (< start)
            #   If (> end)
            #       Case 3
            #   Else (<= end)
            #       Case 2
            # Else (>= start)
            #   If (> end)
            #       Case 1
            #   Else (<= end)
            #       Case 4
            #

            # We convert the nested conditionals into when.then.otherwise expression
            # ----------------------------------------------------------------------
            in_bin_frame = in_bin_frame.with_columns(
                nw.when(nw.col('Timestamp (ns)') < start).then(
                    nw.when(nw.col('_matching_timestamp') > end)
                    .then(end - start) # Case 3
                    .otherwise(nw.col('_matching_timestamp') - start) #Case 2
                ).otherwise(
                    nw.when(nw.col('_matching_timestamp') > end)
                    .then(end - nw.col('Timestamp (ns)')) # Case 1
                    .otherwise(nw.col('_matching_timestamp') - nw.col('Timestamp (ns)')) # Case 4
                ).alias('inc_time_in_bin')
            )

            # Calculate exc_time_in_bin by subtracting inc_time_in_bin for all children
            in_bin_frame = calc_exc_time_in_bin(in_bin_frame)

            # Add bin_id column to act as index and keep track of the bin
            agg = in_bin_frame.group_by('Name').agg(nw.sum('exc_time_in_bin')).with_columns(nw.lit(i).alias('bin_id'))

            # idle_time = total_bin_duration - agg['exc_time_in_bin'].sum()
            # agg = agg.with_columns(nw.lit(idle_time).alias('idle_time'))

            profile.append(agg)


        # Pivot on Name, making the Name values the columns, the bin_id the index,
        # and the exc_time_in_bin the values
        frame = nw.concat(profile).pivot(on='Name', index='bin_id', values='exc_time_in_bin')

        # Get the column names (function names)
        names = frame.columns
        names.remove('bin_id')

        # Fill in missing columns with 0
        frame = frame.with_columns(nw.col(names).fill_null(0))

        # Add idle_time column
        frame = frame.with_columns((total_bin_duration - nw.sum_horizontal(names)).alias('idle_time'))

        # Threshold for zero
        # df.mask(df < 0.01, 0, inplace=True)

        # Normalize
        if normalized:
            frame = frame.with_columns((nw.col(names + ['idle_time']) / total_bin_duration))
            bin_size = bin_size / total_bin_duration

        # Add bin_start and bin_end
        frame = frame.with_columns([
            (nw.col('bin_id') * bin_size).alias('bin_start'),
            (nw.col('bin_id')* bin_size + bin_size).alias('bin_end')
        ])

        return frame

    @staticmethod
    def multirun_analysis(
        traces, metric_column="Timestamp (ns)", groupby_column="Name"
    ):
        """
        Arguments:
        traces - list of pipit traces
        metric_column - the column of the metric to be aggregated over
        groupby_column - the column that will be grouped by before aggregation

        Returns:
        A Pandas DataFrame indexed by the number of processes in the traces, the
        columns are the groups of the groupby_column, and the entries of the DataFrame
        are the aggregated metrics corresponding to the respective trace and group
        """

        # for each trace, collect a flat profile
        flat_profiles = []
        for trace in traces:
            trace.calc_exc_metrics([metric_column])
            metric_col = (
                "time.exc"
                if metric_column == "Timestamp (ns)"
                else metric_column + ".exc"
            )
            flat_profiles.append(
                trace.flat_profile(metrics=[metric_col], groupby_column=groupby_column)
            )

        # combine these flat profiles and index them by number of processes
        combined_df = pd.concat([fp[metric_col] for fp in flat_profiles], axis=1).T
        combined_df.index = [len(set(trace.events["Process"])) for trace in traces]
        combined_df.index.rename("Number of Processes", inplace=True)

        # sort the columns/groups in descending order of the aggregated metric values
        function_sums = combined_df.sum()
        combined_df = combined_df[function_sums.sort_values(ascending=False).index]

        return combined_df

    def detect_pattern(
        self,
        start_event,
        iterations=None,
        window_size=None,
        process=0,
        metric="time.exc",
    ):
        import stumpy

        enter_events = self.events[
            (self.events["Name"] == start_event)
            & (self.events["Event Type"] == "Enter")
            & (self.events["Process"] == process)
        ]

        leave_events = self.events[
            (self.events["Name"] == start_event)
            & (self.events["Event Type"] == "Leave")
            & (self.events["Process"] == process)
        ]

        # count the number of enter events to
        # determine the number of iterations if it's not
        # given by the user.
        if iterations is None:
            iterations = len(enter_events)

        # get the first enter and last leave of
        # the given event. we will only investigate
        # this portion of the data.
        first_loop_enter = enter_events.index[0]
        last_loop_leave = leave_events.index[-1]

        df = self.events.iloc[first_loop_enter + 1 : last_loop_leave]
        filtered_df = df.loc[(df[metric].notnull()) & (df["Process"] == process)]
        y = filtered_df[metric].values[:]

        if window_size is None:
            window_size = int(len(y) / iterations)

        matrix_profile = stumpy.stump(y, window_size)
        dists, indices = stumpy.motifs(y, matrix_profile[:, 0], max_matches=iterations)

        # Gets the corresponding portion from the original
        # dataframe for each pattern.
        patterns = []
        for idx in indices[0]:
            end_idx = idx + window_size

            match_original = self.events.loc[
                self.events["Timestamp (ns)"].isin(
                    filtered_df.iloc[idx:end_idx]["Timestamp (ns)"].values
                )
            ]
            patterns.append(match_original)

        return patterns
