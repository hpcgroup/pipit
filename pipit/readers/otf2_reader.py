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

    # handles otf2 and _otf2 objects
    def fieldToVal(self, defField):
        fieldType = str(type(defField))
        if "otf2.definitions" in fieldType:
            # reference id of the nested object
            return fieldType[25:-2] + " " + str(getattr(defField, "_ref"))
        elif "_otf2.Events" in fieldType:
            return str(defField)
        else:
            return defField

    # handles different data types
    def handleData(self, data):
        if isinstance(data, list):
            return [self.fieldToVal(dataElement) for dataElement in data]
        elif isinstance(data, tuple):
            return tuple([self.fieldToVal(dataElement) for dataElement in data])
        elif isinstance(data, dict):
            return {self.fieldToVal(dataKey): self.fieldToVal(dataValue)
                    for dataKey, dataValue in data.items()}
        else:
            return self.fieldToVal(data)

    # converts the fields of a definition object to a dictionary
    def fieldsToDict(self, defObject):
        fieldsDict = {}
        for field in defObject._fields:
            fieldName = str(field.name)
            fieldsDict[fieldName] = self.handleData(getattr(defObject, fieldName))

        if len(fieldsDict) == 1:  # collapse single dictionaries to a string
            return list(fieldsDict.values())[0]
        else:
            return fieldsDict

    # serial events reader
    def events_reader(self, rank_size):
        with otf2.reader.open(self.dir_name) as trace:
            rank, size = rank_size[0], rank_size[1]
            locations = list(trace.definitions._locations)

            perProcess = math.floor(len(locations) / size)
            beginInt, endInt = int(rank * perProcess), int((rank + 1) * perProcess)

            timestamps, eventTypes, eventAttributes, names = [], [], [], []
            locs, locTypes, locGroups, locGroupTypes = [], [], [], []

            # selects locations based on the rank
            if rank == size - 1:
                loc_events = list(trace.events(locations[beginInt:]).__iter__())
            elif len(locations[beginInt:endInt]) != 0:
                loc_events = list(trace.events(locations[beginInt: endInt]).__iter__())

            # iterating through events and processing them
            for loc_event in loc_events:
                loc, event = loc_event[0], loc_event[1]

                locs.append(loc._ref)
                locTypes.append(str(loc.type)[13:])
                locGroups.append(loc.group._ref)
                locGroupTypes.append(str(loc.group.location_group_type)[18:])

                eventType = str(type(event))[20:-2]
                eventTypes.append(eventType)

                if eventType in ["Enter", "Leave"]:
                    names.append(event.region.name)
                else:
                    names.append("N/A")

                timestamps.append(event.time)

                if eventType != "Leave":
                    attributesDict = {}
                    for key, value in vars(event).items():
                        if value is not None and key != "time":
                            attributesDict[self.fieldToVal(key)] = self.handleData(
                                                                   value)
                    eventAttributes.append(attributesDict)
                else:
                    # no need for duplicate attributes
                    eventAttributes.append(float("NaN"))

            trace.close()  # close event files

        # returns dictionary with all events and their fields
        return {"Event": eventTypes, "Timestamp (ns)": timestamps, "Name": names,
                "Location ID": locs, "Location Type": locTypes,
                "Location Group ID": locGroups, "Location Group Type": locGroupTypes,
                "Attributes": eventAttributes}

    # writes the definitions to a Pandas DataFrame
    def defToDF(self, trace):
        # ids are for objects stored in a reference registry
        definitions, defIds, attributes = [], [], []

        # iterating through definition registry attributes
        for key in vars(trace.definitions).keys():
            defAttr = getattr(trace.definitions, str(key))
            if key == "clock_properties":  # only definition type that is not a registry
                defIds.append("")
                definitions.append(str(type(defAttr))[25:-2])
                # converts a definition object to a dictionary of its attributes
                attributes.append(self.fieldsToDict(defAttr))
            elif "otf2" not in key:
                # iterate through registry elements
                for defObject in defAttr.__iter__():
                    try:
                        defIds.append(defObject._ref)
                    except Exception:
                        defIds.append("")
                    definitions.append(str(type(defObject))[25:-2])
                    attributes.append(self.fieldsToDict(defObject))

        # return the definitions as a DataFrame
        defDF = pd.DataFrame({"Definition": definitions,
                              "ID": defIds, "Attributes": attributes})

        return defDF

    # writes the events to a Pandas DataFrame
    def eventsToDF(self):
        # parallelizes the reading of events
        poolSize, pool = mp.cpu_count(), mp.Pool()
        eventsDict = pool.map(self.events_reader, [(
                     rank, poolSize) for rank in range(poolSize)])

        # combines results from each process
        for i in range(len(eventsDict) - 1):
            for key, value in eventsDict[0].items():
                value.extend(eventsDict[1][key])
            del eventsDict[1]
        eventsDict = eventsDict[0]

        # returns the events as a DataFrame
        eventsDF = pd.DataFrame(eventsDict)

        # cleaning up timestamps
        clockProps = self.definitions.loc[self.definitions[
                     "Definition"] == "ClockProperties"]["Attributes"].values[0]
        offset, resolution = clockProps["global_offset"], clockProps["timer_resolution"]
        eventsDF["Timestamp (ns)"] -= offset
        eventsDF["Timestamp (ns)"] *= ((10**9) / resolution)

        # ensures the DataFrame is in order of increasing timestamp
        eventsDF.sort_values(by="Timestamp (ns)", axis=0,
                             ascending=True, inplace=True, ignore_index=True)

        # using categorical dtypes for memory optimization
        eventsDF = eventsDF.astype({"Event": "category", "Name": "category",
                                    "Location ID": "category",
                                    "Location Type": "category",
                                    "Location Group ID": "category",
                                    "Location Group Type": "category"})

        return eventsDF

    # returns a tuple containing definitions and events
    def read(self):
        with otf2.reader.open(self.dir_name) as trace:
            self.definitions = self.defToDF(trace)
            trace.close()
        self.events = self.eventsToDF()
        return pipit.tracedata.TraceData(self.definitions, self.events)
