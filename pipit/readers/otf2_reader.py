# Copyright 2022-2023 Parallel Software and Systems Group, University of
# Maryland. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import otf2
import numpy as np
import pandas as pd
import multiprocessing as mp
import pipit.trace


class OTF2Reader:
    """Reader for OTF2 trace files"""

    def __init__(self, dir_name, num_processes=None):
        self.dir_name = dir_name  # directory of otf2 file being read
        self.file_name = self.dir_name + "/traces.otf2"

        num_cpus = mp.cpu_count()
        if num_processes is None or num_processes < 1 or num_processes > num_cpus:
            # uses all processes to parallelize reading by default
            self.num_processes = num_cpus
        else:
            self.num_processes = num_processes

    def field_to_val(self, field):
        """
        Handles otf2 and _otf2 objects

        Arguments:
        field: an otf2 object, _otf2 object, or any other field
        that can have different data types such as strings, ints, etc

        Returns:
        if otf2 definition, a string representation of the definition and
        its ID such as "Region 19" that the user can use to refer back
        to the definitions dataframe
        else if other otf2 or _otf2 objects, a simple string representation of
        the object
        else don't make any changes

        This function also ensures that there is no pickling of otf2 or _otf2
        objects, which could cause errors
        """

        """
        Note: any occurrence of [25:-2] or something similar
        is some simple string manipulation to only extract the relevant
        part of the string and not information like the type such as
        otf2.definitions, etc
        """

        field_type = str(type(field))
        if "otf2.definitions" in field_type:
            """
            Example: An event can have an attribute called region which corresponds
            to a definition. We strip the string and extract only the relevant
            information, which is the type of definition such as Region and also
            append its id (like Region 6) so that this definition can be accessed
            in the Definitions DataFrame
            """
            return field_type[25:-2] + " " + str(getattr(field, "_ref"))
        elif "_otf2" in field_type or "otf2" in field_type:
            """
            Example: A measurement event has an attribute called measurement mode
            which is either MeasurementMode.OFF or MeasurementMode.ON. These are not
            definitions, but they are an object in the lower level _otf2 library,
            and to ensure no pickling errors, I convert these objects to their
            string representation
            """
            return str(field)
        else:
            "not an otf2 type, then just return normally"
            return field

    def handle_data(self, data):
        """
        Handles different data structures

        Arguments:
        data: could be a list, tuple, set, dict, or any other python data type

        Returns:
        the same data structure as the passed argument but field_to_val is applied
        to all of the values it contains

        Note: all of the below cases handle the case where the data structure
        could be nested, which is always possibility depending on the trace's
        specific attributes
        """

        if isinstance(data, list):
            return [self.handle_data(data_element) for data_element in data]
        elif isinstance(data, dict):
            """
            Example: ProgramBegin events have an attribute that is a definition
            and quite ironically, also known as attribute. These are stored in
            a dictionary where the key is a definition like "Attribute 2" and
            the integer like 15968
            """
            return {
                self.field_to_val(data_key): self.handle_data(data_value)
                for data_key, data_value in data.items()
            }
        elif isinstance(data, tuple):
            """
            Example: There is a definition called CartTopology which has a
            field called dimensions that is a tuple of two other definitions
            called CartDimensions, showing why this nested structure is needed
            """
            return tuple([self.handle_data(data_element) for data_element in data])
        elif isinstance(data, set):
            """
            Haven't encountered this type, but added just in case any situations like
            the above ones do arise for this data type
            """
            return set([self.handle_data(data_element) for data_element in data])
        else:
            "this represents the case for most fields/attributes"
            return self.field_to_val(data)

    def fields_to_dict(self, def_object):
        """
        converts the fields in the attribute column of a definition
        object to a dictionary
        """

        fields_dict = {}
        # iterates through the fields of the definition
        # (ex: region has fields like name, paradigm source file, etc)
        for field in def_object._fields:
            field_name = str(field.name)
            # use the handle_data function to process the field's data appropriately
            fields_dict[field_name] = self.handle_data(getattr(def_object, field_name))

        if len(fields_dict) == 1:
            # collapse single dictionaries to a value
            return list(fields_dict.values())[0]
        else:
            return fields_dict

    def events_reader(self, rank_size):
        """
        Serial events reader that reads a subset of the trace

        Arguments:
        rank_size: a tuple containing the rank of the process
        and the size/total number of processors that are being used

        Returns:
        a dictionary with a subset of the trace events that can be converted
        to a dataframe
        """

        with otf2.reader.open(self.file_name) as trace:
            # extracts the rank and size
            # and gets all the locations
            # of the trace
            rank, size = rank_size[0], rank_size[1]
            locations = list(trace.definitions._locations)
            num_locations = len(locations)

            # base number of locations read by each process
            per_process = int(num_locations // size)

            # remainder number of locations to be split evenly
            remainder = int(num_locations % size)

            if rank < remainder:
                """
                Example:
                For the reading of 30 locations split over 14 processes,
                first 2 processes will read 3 locations each since the remainder
                is 2.
                """
                begin_int = rank * (per_process + 1)
                end_int = (rank + 1) * (per_process + 1)
            else:
                """
                Example:
                For the reading of 30 locations split over 14 processes,
                last 12 processes will read 2 locations each. The starting index
                accounts for the fact that the first two will read 3 locations each.
                """
                begin_int = (rank * per_process) + remainder
                end_int = ((rank + 1) * per_process) + remainder

            # select the locations to read based on above calculations
            loc_events = list(trace.events(locations[begin_int:end_int]).__iter__())

            # columns of the DataFrame
            timestamps, event_types, event_attributes, names = [], [], [], []

            # note: the below lists are for storing logical ids
            process_ids, thread_ids = [], []

            # list of metric members of in the definitions
            metric_names = set(
                self.definitions.loc[
                    self.definitions["Definition Type"] == "MetricMember"
                ]["Attributes"]
                .map(lambda attr: attr["name"])
                .to_list()
            )

            # maps each metric to a list of its values
            metrics_dict = {metric_name: [] for metric_name in metric_names}

            # used to keep track of information about
            # the most recent metrics that were read
            prev_metric_time = -1
            prev_metrics = set()

            # iterates through the events and processes them
            for loc_event in loc_events:
                # extracts the location and event
                # location could be thread, process, etc
                loc, event = loc_event[0], loc_event[1]

                # To Do:
                # Support for GPU events has to be
                # added and unified across readers.
                if str(loc.type)[13:] == "CPU_THREAD":
                    # don't add metric events as a separate row,
                    # and add their values into columns instead
                    if type(event) == otf2.events.Metric:
                        # Since the location is a cpu thread, we know
                        # that the metric event is of type MetricClass,
                        # which has a list of MetricMembers.
                        metrics = list(
                            map(lambda metric: metric.name, event.metric.members)
                        )
                        metric_values = event.values

                        # append the values for the metrics
                        # to their appropriate lists
                        for i in range(len(metrics)):
                            metrics_dict[metrics[i]].append(metric_values[i])

                        # store the metrics and their timestamp
                        prev_metric_time = event.time
                        prev_metrics = set(metrics)
                    else:
                        # MetricClass metric events are synchronous
                        # and coupled with an enter or leave event that
                        # has the same timestamp,
                        if event.time == prev_metric_time:
                            # once an enter or leave that is paired with some metrics
                            # is encountered, add placeholders for all the metric lists
                            # which aren't paired with this event
                            for metric in metric_names - prev_metrics:
                                metrics_dict[metric].append(float("nan"))
                        else:
                            # if the event is not paired with any metric, then
                            # add placeholders for all the metric lists
                            for metric in metric_names:
                                metrics_dict[metric].append(float("nan"))

                        # reset these as a metric event was not read
                        prev_metric_time = -1
                        prev_metrics = set()

                        """
                        Below is code to read the primary information about the
                        non-metric event, such as location, attributes, etc.
                        """

                        process_id = loc.group._ref
                        process_ids.append(process_id)

                        # subtract the minimum location number of a process
                        # from the location number to get threads numbered
                        # 0 to (num_threads per process - 1) for each process.
                        thread_ids.append(
                            loc._ref - self.process_threads_map[process_id]
                        )

                        # type of event - enter, leave, or other types
                        event_type = str(type(event))[20:-2]
                        if event_type == "Enter" or event_type == "Leave":
                            event_types.append(event_type)
                        else:
                            event_types.append("Instant")

                        if event_type in ["Enter", "Leave"]:
                            names.append(event.region.name)
                        else:
                            names.append(event_type)

                        timestamps.append(event.time)

                        # only add attributes for non-leave rows so that
                        # there aren't duplicate attributes for a single event
                        if event_type != "Leave":
                            attributes_dict = {}

                            # iterates through the event's attributes
                            # (ex: region, bytes sent, etc)
                            for key, value in vars(event).items():
                                # only adds non-empty attributes
                                # and ignores time so there isn't a duplicate time
                                if value is not None and key != "time":
                                    # uses field_to_val to convert all data types
                                    # and ensure that there are no pickling errors
                                    attributes_dict[
                                        self.field_to_val(key)
                                    ] = self.handle_data(value)
                            event_attributes.append(attributes_dict)
                        else:
                            # nan attributes for leave rows
                            # attributes column is of object dtype
                            event_attributes.append(None)

            trace.close()  # close event files

        # returns dataframe with all events and their fields
        trace_df = pd.DataFrame(
            {
                "Timestamp (ns)": timestamps,
                "Event Type": event_types,
                "Name": names,
                "Thread": thread_ids,
                "Process": process_ids,
                "Attributes": event_attributes,
            }
        )

        for metric, metric_values in metrics_dict.items():
            if not np.isnan(metric_values).all():
                trace_df[metric] = metric_values

        return trace_df

    def read_definitions(self, trace):
        """
        Reads the definitions from the trace and converts them to a Pandas
        DataFrame
        """

        # OTF2 stores locations numbered from 0 to the (total number of threads - 1)
        # across all processes. This dict will help us convert those to be orderered
        # from 0 to (number of threads for each process - 1) per process instead.
        self.process_threads_map = dict()

        # ids are the _ref attribute of an object
        # all objects stored in a reference registry
        # (such as regions) have such an id
        def_name, def_id, attributes = [], [], []

        # iterating through definition registry attributes
        # such as regions, strings, locations, etc
        for key in vars(trace.definitions).keys():
            # current attribute such as region, string, etc
            def_attribute = getattr(trace.definitions, str(key))

            # only definition type that is not a registry
            if key == "clock_properties":
                # clock properties doesn't have an ID
                def_id.append(float("NaN"))
                def_name.append(str(type(def_attribute))[25:-2])
                attributes.append(self.fields_to_dict(def_attribute))

            # ignores otf2 wrapper properties (don't provide useful info)
            elif "otf2" not in key:
                """
                iterate through registry elements
                (ex: iterating through all regions
                if region is the current definition)
                def_object is a single object of that definition
                type for example, if def_attribute is regions,
                then def_object is a single region being looked at
                """
                for def_object in def_attribute.__iter__():
                    # add to process threads map dict if you encounter a new location
                    if (
                        key == "_locations"
                        and str(def_object.type) == "LocationType.CPU_THREAD"
                    ):
                        location_num, process_num = (
                            def_object._ref,
                            def_object.group._ref,
                        )

                        # each process (location group) will be mapped to its
                        # minimum location number, which we will use to number threads
                        # appropriately by subtracting that min from its location nums
                        if process_num not in self.process_threads_map:
                            self.process_threads_map[process_num] = location_num
                        elif location_num < self.process_threads_map[process_num]:
                            self.process_threads_map[process_num] = location_num

                    if hasattr(def_object, "_ref"):
                        # only add ids for those definitions that have it
                        def_id.append(def_object._ref)
                    else:
                        # ID column is of float64 dtype
                        def_id.append(float("NaN"))

                    # name of the definition
                    def_name.append(str(type(def_object))[25:-2])

                    # converts a definition object to a dictionary of its attributes
                    # this contains information that a user would have to access the
                    # definitions DataFrame for
                    attributes.append(self.fields_to_dict(def_object))

        # return the definitions as a DataFrame
        definitions_dataframe = pd.DataFrame(
            {"Definition Type": def_name, "ID": def_id, "Attributes": attributes}
        )

        # Definition column is of categorical dtype
        definitions_dataframe = definitions_dataframe.astype(
            {"Definition Type": "category"}
        )

        return definitions_dataframe

    def read_events(self):
        """
        Writes the events to a Pandas DataFrame
        using the multiprocessing library and the events_reader
        function
        """

        # parallelizes the reading of events
        # using the multiprocessing library
        pool_size, pool = self.num_processes, mp.Pool(self.num_processes)

        # list of dataframes returned by the processes pool
        events_dataframes = pool.map(
            self.events_reader, [(rank, pool_size) for rank in range(pool_size)]
        )

        pool.close()

        # merges the dataframe into one events dataframe
        events_dataframe = pd.concat(events_dataframes)
        del events_dataframes

        # accessing the clock properties of the trace using the definitions
        clock_properties = self.definitions.loc[
            self.definitions["Definition Type"] == "ClockProperties"
        ]["Attributes"].values[0]
        offset, resolution = (
            clock_properties["global_offset"],
            clock_properties["timer_resolution"],
        )

        # shifting the timestamps by the global offset
        # and dividing by the resolution to convert to nanoseconds
        # as per OTF2's website
        events_dataframe["Timestamp (ns)"] -= offset
        events_dataframe["Timestamp (ns)"] *= (10**9) / resolution

        # ensures the DataFrame is in order of increasing timestamp
        events_dataframe.sort_values(
            by="Timestamp (ns)", axis=0, ascending=True, inplace=True, ignore_index=True
        )

        # convert these to ints
        # (sometimes they get converted to floats
        #  while concatenating dataframes)
        events_dataframe = events_dataframe.astype(
            {"Thread": "int32", "Process": "int32"}
        )

        # using categorical dtypes for memory optimization
        # (only efficient when used for categorical data)
        events_dataframe = events_dataframe.astype(
            {
                "Event Type": "category",
                "Name": "category",
                "Thread": "category",
                "Process": "category",
            }
        )

        return events_dataframe

    def read(self):
        """
        Returns a Trace object for the otf2 file
        that has one definitions DataFrame and another
        events DataFrame as its primary attributes
        """

        with otf2.reader.open(self.file_name) as trace:  # noqa: F821
            self.definitions = self.read_definitions(trace)  # definitions

            # if a trace has n locations, we should only parallelize
            # the reading of events over a number of processes
            # equal to n at a maximum
            num_locations = len(trace.definitions._locations)
            if self.num_processes > num_locations:
                self.num_processes = num_locations

            # close the trace and open it later per process
            trace.close()

        self.events = self.read_events()  # events

        return pipit.trace.Trace(self.definitions, self.events)
