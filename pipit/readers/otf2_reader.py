# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import math
import otf2
import pandas as pd
import multiprocessing as mp
import pipit.tracedata


class OTF2Reader:
    """Reader for OTF2 trace files"""

    def __init__(self, dir_name):
        self.dir_name = dir_name  # directory of otf2 file being read

    def fieldToVal(self, field):
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

        fieldType = str(type(field))
        if "otf2.definitions" in fieldType:
            # reference id of the nested object
            return fieldType[25:-2] + " " + str(getattr(field, "_ref"))
        elif "_otf2" in fieldType or "otf2" in fieldType:
            return str(field)
        else:
            return field

    def handleData(self, data):
        """
        Handles different data structures

        Arguments:
        data: could be a list, tuple, set, dict, or any other python data type

        Returns:
        the same data structure as the passed argument but fieldToVal is applied
        to all of the values it contains
        """

        if isinstance(data, list):
            return [self.handleData(dataElement) for dataElement in data]
        elif isinstance(data, dict):
            return {
                self.fieldToVal(dataKey): self.handleData(dataValue)
                for dataKey, dataValue in data.items()
            }
        elif isinstance(data, tuple):
            return tuple([self.handleData(dataElement) for dataElement in data])
        elif isinstance(data, set):
            return set([self.handleData(dataElement) for dataElement in data])
        else:
            return self.fieldToVal(data)

    def fieldsToDict(self, defObject):
        """
        converts the fields in the attribute column of a definition
        object to a dictionary
        """

        fieldsDict = {}
        # iterates through the fields of the definition
        for field in defObject._fields:
            fieldName = str(field.name)
            fieldsDict[fieldName] = self.handleData(getattr(defObject, fieldName))

        if len(fieldsDict) == 1:
            # collapse single dictionaries to a value
            return list(fieldsDict.values())[0]
        else:
            return fieldsDict

    def events_reader(self, rank_size):
        """
        Serial events reader

        Arguments:
        rank_size: a tuple containing the rank of the process
        and the size/total number of processors that are being used

        Returns:
        a dictionary with a subset of the trace events that can be converted
        to a dataframe
        """

        with otf2.reader.open(self.dir_name) as trace:
            # extracts the rank and size
            # and gets all the locations
            # of the trace
            rank, size = rank_size[0], rank_size[1]
            locations = list(trace.definitions._locations)

            # calculates how many locations to read per process
            # and determines starting and ending indices to select
            # for the current process
            perProcess = math.floor(len(locations) / size)
            beginInt, endInt = int(rank * perProcess), int((rank + 1) * perProcess)

            timestamps, eventTypes, eventAttributes, names = [], [], [], []
            locs, locTypes, locGroups, locGroupTypes = [], [], [], []

            # selects a subset of all locations to
            # read based on the current rank
            loc_events = []
            if rank == size - 1:
                loc_events = list(trace.events(locations[beginInt:]).__iter__())
            elif len(locations[beginInt:endInt]) != 0:
                loc_events = list(trace.events(locations[beginInt:endInt]).__iter__())

            # iterates through the events and processes them
            for loc_event in loc_events:
                # extracts the location and event
                loc, event = loc_event[0], loc_event[1]

                # information about the location
                # that the event occurred on
                locs.append(loc._ref)
                locTypes.append(str(loc.type)[13:])
                locGroups.append(loc.group._ref)
                locGroupTypes.append(str(loc.group.location_group_type)[18:])

                # type of event - enter, leave, or other types
                eventType = str(type(event))[20:-2]
                eventTypes.append(eventType)

                # only enter/leave events have a name
                if eventType in ["Enter", "Leave"]:
                    names.append(event.region.name)
                else:
                    # names column is of categorical dtype
                    names.append("N/A")

                timestamps.append(event.time)

                # only add attributes for non-leave rows so that
                # there aren't duplicate attributes for a single event
                if eventType != "Leave":
                    attributesDict = {}
                    # iterates through the event's attributes
                    for key, value in vars(event).items():
                        # only adds non-empty attributes
                        # and ignores time so there isn't a duplicate time
                        if value is not None and key != "time":
                            # uses fieldToVal to convert all data types appropriately
                            # and ensure that there are no pickling errors
                            attributesDict[self.fieldToVal(key)] = self.handleData(
                                value
                            )
                    eventAttributes.append(attributesDict)
                else:
                    # nan attributes for leave rows
                    # attributes column is of object dtype
                    eventAttributes.append(None)

            trace.close()  # close event files

        # returns dictionary with all events and their fields
        return {
            "Event": eventTypes,
            "Timestamp (ns)": timestamps,
            "Name": names,
            "Location ID": locs,
            "Location Type": locTypes,
            "Location Group ID": locGroups,
            "Location Group Type": locGroupTypes,
            "Attributes": eventAttributes,
        }

    def read_definitions(self, trace):
        """
        Reads the definitions from the trace and converts them to a Pandas
        DataFrame """

        # ids are the _ref attribute of an object
        # all objects stored in a reference registry
        # (such as regions) have such an id
        def_name, def_id, attributes = [], [], []

        # iterating through definition registry attributes
        # such as regions, strings, locations, etc
        for key in vars(trace.definitions).keys():
            # 
            defAttr = getattr(trace.definitions, str(key))

            # only definition type that is not a registry
            if key == "clock_properties":
                def_id.append(float("NaN"))
                # strip out "otf2.definitions."
                def_name.append(str(type(defAttr))[25:-2])
                attributes.append(self.fieldsToDict(defAttr))
            elif "otf2" not in key:  # otf2 wrapper properties (not needed)
                # iterate through registry elements
                # (ex: iterating through all regions
                # if region is the current definition)
                for defObject in defAttr.__iter__():
                    if hasattr(defObject, "_ref"):
                        # only add ids for those that have it
                        def_id.append(defObject._ref)
                    else:
                        # ID column is of float64 dtype
                        def_id.append(float("NaN"))

                    # name of the definition
                    def_name.append(str(type(defObject))[25:-2])

                    # converts a definition object to a dictionary of its attributes
                    # this contains information that a user would have to access the
                    # definitions DataFrame for
                    attributes.append(self.fieldsToDict(defObject))

        # return the definitions as a DataFrame
        defDF = pd.DataFrame(
            {"Definition": def_name, "ID": def_id, "Attributes": attributes}
        )

        # Definition column is of categorical dtype
        defDF = defDF.astype({"Definition": "category"})

        return defDF

    def read_events(self):
        """
        Writes the events to a Pandas DataFrame
        using the multiprocessing library and the events_reader
        function
        """

        # parallelizes the reading of events
        # using the multiprocessing library
        poolSize, pool = mp.cpu_count(), mp.Pool()
        eventsDict = pool.map(
            self.events_reader, [(rank, poolSize) for rank in range(poolSize)]
        )

        # combines the dictionaries returned from each
        # process to generate a full trace
        for i in range(len(eventsDict) - 1):
            for key, value in eventsDict[0].items():
                value.extend(eventsDict[1][key])
            del eventsDict[1]
        eventsDict = eventsDict[0]

        # returns the events as a DataFrame
        eventsDF = pd.DataFrame(eventsDict)

        # cleaning up timestamps
        clockProps = self.definitions.loc[
            self.definitions["Definition"] == "ClockProperties"
        ]["Attributes"].values[0]
        offset, resolution = clockProps["global_offset"], clockProps["timer_resolution"]

        # shifting the timestamps by the offset and
        # converting them to nanoseconds
        eventsDF["Timestamp (ns)"] -= offset
        eventsDF["Timestamp (ns)"] *= (10**9) / resolution

        # ensures the DataFrame is in order of increasing timestamp
        eventsDF.sort_values(
            by="Timestamp (ns)", axis=0, ascending=True, inplace=True, ignore_index=True
        )

        # using categorical dtypes for memory optimization
        # (only efficient when used for categorical data)
        eventsDF = eventsDF.astype(
            {
                "Event": "category",
                "Name": "category",
                "Location ID": "category",
                "Location Type": "category",
                "Location Group ID": "category",
                "Location Group Type": "category",
            }
        )

        return eventsDF

    def read(self):
        """
        Returns a TraceData object for the otf2 file
        that has one definitions DataFrame and another
        events DataFrame as its primary attributes
        """

        with otf2.reader.open(self.dir_name) as trace:
            self.definitions = self.read_definitions(trace)  # definitions
            # close the trace and open it later per process
            trace.close()
        self.events = self.read_events()  # events
        return pipit.tracedata.TraceData(self.definitions, self.events)
