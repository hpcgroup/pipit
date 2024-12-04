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

        # will store columns names for inc/exc metrics
        self.inc_metrics = []
        self.exc_metrics = []

    def create_cct(self):
        # adds a column of cct nodes to the events dataframe
        # and stores the graph object in self.cct
        self.cct = create_cct(self.events)

    @staticmethod
    def from_otf2(dirname, num_processes=None, create_cct=False):
        """Read an OTF2 trace into a new Trace object."""
        # import this lazily to avoid circular dependencies
        from .readers.otf2_reader import OTF2Reader

        return OTF2Reader(dirname, num_processes, create_cct).read()

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
        columns = columns.copy()

        # get the corresponding metric column name for columns provided,
        # ignoring the columns that have already been calculated
        tmp_leave_col_names = []
        for col_name in columns:
            metric_col_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            if metric_col_name in self.events.columns:
                columns.remove(col_name)
            else:
                tmp_leave_col_names.append("leave_" + col_name)
        if len(columns) == 0:
            return

        # Get only leave events
        leave_frame = self.events.filter(nw.col('Event Type') == 'Leave')
        # Select only the needed columns (metric columns and matching event)
        leave_frame = leave_frame.rename({col_name: "leave_" + col_name for col_name in columns}).select(
            ["_matching_event"] + tmp_leave_col_names
        )

        # Join the leave events with the original events, creating a new column for each metric
        # populated with the value of the metric at the leave event
        self.events = self.events.join(leave_frame, left_on='unique_id', right_on='_matching_event',
                                       how="left")
        # Create list of expressions to calculate the inclusive metrics
        # Each expression is (metric_value_at_leave - metric_value_at_enter)
        exp_list = []
        for col_name in columns:
            metric_col_inc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            exp_list.append((nw.col("leave_"+col_name) - nw.col(col_name)).alias(metric_col_inc_name))
        self.events = self.events.with_columns(exp_list).drop(tmp_leave_col_names)


    def calc_exc_metrics(self, columns=None):
        # calculate exc metrics for all numeric columns if not specified
        columns = self.numeric_cols if columns is None else columns

        # match caller and callee rows
        self._match_caller_callee()

        # calculate inclusive metrics if needed
        self.calc_inc_metrics(columns)

        # Create list of aggregations to do (each metric)
        exp_list = []
        # create list of new column names for the sum of inclusive metrics
        metric_col_inc_names = []
        for col_name in columns:
            metric_col_inc_name = ("time" if col_name == "Timestamp (ns)" else col_name) + ".inc"
            exp_list.append(nw.col(metric_col_inc_name).sum().alias('child_' + metric_col_inc_name))
            metric_col_inc_names.append(metric_col_inc_name)
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
        for col_name in columns:
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

        # Filter by send events
        messages = self.events[self.events["Name"].isin(["MpiSend", "MpiIsend"])]

        # Get message sizes
        sizes = messages["Attributes"].map(lambda x: x["msg_length"])

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
        events = self.events[
            self.events["Name"].isin(
                ["MpiSend", "MpiIsend"]
                if message_type == "send"
                else ["MpiRecv", "MpiIrecv"]
            )
        ]

        # Get timestamps and sizes
        timestamps = events["Timestamp (ns)"]
        sizes = events["Attributes"].apply(lambda x: x["msg_length"])

        return np.histogram(
            timestamps,
            bins=bins,
            weights=sizes.tolist() if output == "size" else None,
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
        if per_process:
            return (
                self.events.loc[self.events["Event Type"] == "Enter"]
                .groupby([groupby_column, "Process"], observed=True)[metrics]
                .sum()
            )
        else:
            return (
                self.events.loc[self.events["Event Type"] == "Enter"]
                .groupby([groupby_column, "Process"], observed=True)[metrics]
                .sum()
                .groupby(groupby_column)
                .mean()
            )

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

        flat_profile = self.flat_profile(metrics=metric, per_process=True)

        imbalance_dict = dict()

        imb_metric = metric + ".imbalance"
        imb_ranks = "Top processes"
        mean_metric = metric + ".mean"

        imbalance_dict[imb_metric] = []
        imbalance_dict[imb_ranks] = []
        imbalance_dict[mean_metric] = []

        functions = set(self.events.loc[self.events["Event Type"] == "Enter"]["Name"])
        for function in functions:
            curr_series = flat_profile.loc[function]

            top_n = curr_series.sort_values(ascending=False).iloc[0:num_display]

            imbalance_dict[mean_metric].append(curr_series.mean())
            imbalance_dict[imb_metric].append(top_n.values[0] / curr_series.mean())
            imbalance_dict[imb_ranks].append(list(top_n.index))

        imbalance_df = pd.DataFrame(imbalance_dict)
        imbalance_df.index = functions
        imbalance_df.sort_values(by=mean_metric, axis=0, inplace=True, ascending=False)

        return imbalance_df

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
        events = self.events[self.events["Event Type"] == "Enter"].copy(deep=False)
        names = events["Name"].unique().tolist()

        # Create equal-sized bins
        edges = np.linspace(
            self.events["Timestamp (ns)"].min(),
            self.events["Timestamp (ns)"].max(),
            num_bins + 1,
        )
        bin_size = edges[1] - edges[0]

        total_bin_duration = bin_size * len(events["Process"].unique())

        profile = []

        def calc_exc_time_in_bin(events):
            # TODO: check if the numpy equivalent of the below code is faster
            dfx_to_idx = {
                dfx: idx
                for (dfx, idx) in zip(events.index, [i for i in range(len(events))])
            }

            # start out with exc times being a copy of inc times
            exc_times = list(events["inc_time_in_bin"].copy(deep=False))

            # filter to events that have children
            filtered_df = events.loc[events["_children"].notnull()]

            parent_df_indices, children = (
                list(filtered_df.index),
                filtered_df["_children"].to_list(),
            )

            # Iterate through the events that are parents
            for i in range(len(filtered_df)):
                curr_parent_idx, curr_children = (
                    dfx_to_idx[parent_df_indices[i]],
                    children[i],
                )

                # Only consider inc times of children in current bin
                for child_df_idx in curr_children:
                    if child_df_idx in dfx_to_idx:
                        exc_times[curr_parent_idx] -= exc_times[
                            dfx_to_idx[child_df_idx]
                        ]

            events["exc_time_in_bin"] = exc_times

        # For each bin, determine each function's time contribution
        for i in range(num_bins):
            start = edges[i]
            end = edges[i + 1]

            # Find functions that belong in this bin
            in_bin = events[
                (events["_matching_timestamp"] > start)
                & (events["Timestamp (ns)"] < end)
            ].copy(deep=False)

            # Calculate inc_time_in_bin for each function
            # Case 1 - Function starts in bin
            in_bin.loc[in_bin["Timestamp (ns)"] >= start, "inc_time_in_bin"] = (
                end - in_bin["Timestamp (ns)"]
            )

            # Case 2 - Function ends in bin
            in_bin.loc[in_bin["_matching_timestamp"] <= end, "inc_time_in_bin"] = (
                in_bin["_matching_timestamp"] - start
            )

            # Case 3 - Function spans bin
            in_bin.loc[
                (in_bin["Timestamp (ns)"] < start)
                & (in_bin["_matching_timestamp"] > end),
                "inc_time_in_bin",
            ] = (
                end - start
            )

            # Case 4 - Function contained in bin
            in_bin.loc[
                (in_bin["Timestamp (ns)"] >= start)
                & (in_bin["_matching_timestamp"] <= end),
                "inc_time_in_bin",
            ] = (
                in_bin["_matching_timestamp"] - in_bin["Timestamp (ns)"]
            )

            # Calculate exc_time_in_bin by subtracting inc_time_in_bin for all children
            calc_exc_time_in_bin(in_bin)

            # Sum across all processes
            agg = in_bin.groupby("Name")["exc_time_in_bin"].sum()
            profile.append(agg.to_dict())

        # Convert to DataFrame
        df = pd.DataFrame(profile, columns=names)

        # Add idle_time column
        df.insert(0, "idle_time", total_bin_duration - df.sum(axis=1))

        # Threshold for zero
        df.mask(df < 0.01, 0, inplace=True)

        # Normalize
        if normalized:
            df /= total_bin_duration

        # Add bin_start and bin_end
        df.insert(0, "bin_start", edges[:-1])
        df.insert(0, "bin_end", edges[1:])

        return df

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
