#import essential libraries
import otf2
import pandas as pd


#reader for otf2 files
class ScorepReader:
    #directory of otf2 file being read
    def __init__(self, dir_name):
        self.dir_name = dir_name


    #converts a single field to an appropriate value
    def fieldToVal(self, defField):
        fieldType = str(type(defField))
        if "otf2.definitions" in fieldType:
            return fieldType[25:-2] + " " + str(getattr(defField, "_ref")) #reference id of the nested object
        else:
            return defField

    #handles different data types and processes them accordingly
    def handleData(self, data):
        if isinstance(data, list):
            return [self.fieldToVal(dataElement) for dataElement in data]
        elif isinstance(data, tuple):
            return tuple([self.fieldToVal(dataElement) for dataElement in data])
        elif isinstance(data, dict):
            return {dataKey: self.fieldToVal(dataValue) for dataKey, dataValue in data.items()}
        else:
            return self.fieldToVal(data)


    #converts the fields of a definition object to a dictionary
    def fieldsToDict(self, defObject):
        fieldsDict = {}
        for field in defObject._fields:
            fieldName = str(field.name) 
            fieldsDict[fieldName] = self.handleData(getattr(defObject, fieldName))
                
        if len(fieldsDict) == 1: #collapse single dictionaries to a string
            return list(fieldsDict.values())[0]
        else:
            return fieldsDict


    #writes a trace's definitions to a Pandas DataFrame
    def defToDF(self, trace):
        definitions, defIds, attributes = [], [], [] #ids are for objects stored in a reference registry
        for key in vars(trace.definitions).keys(): #iterating through definition registry attributes
            defAttr = getattr(trace.definitions, str(key))
            if key == "clock_properties": #only definition type that is not a registry
                defIds.append("")
                definitions.append(str(type(defAttr))[25:-2])
                attributes.append(self.fieldsToDict(defAttr)) #converts a definition object to a dictionary of its attributes
            elif "otf2" not in key:
                for defObject in defAttr.__iter__(): #iterate through registry elements
                    try:
                        defIds.append(defObject._ref)
                    except:
                        defIds.append("")
                    definitions.append(str(type(defObject))[25:-2])
                    attributes.append(self.fieldsToDict(defObject))

        #return the definitions as a DataFrame
        defDF = pd.DataFrame({"Definition": definitions, "ID": defIds, "Attributes": attributes})
        return defDF


    #writes a trace's events to a Pandas DataFrame
    def eventsToDF(self, trace):
            locations, timestamps, eventTypes, eventAttributes, names = [], [], [], [], []
            for location, event in trace.events.__iter__(): #iterate through each event
                locations.append(self.fieldToVal(location))
                eventType = str(type(event))[20:-2]
                eventTypes.append(eventType)

                if eventType in ["Enter", "Leave"]: #adds the name if a function enters and leaves a region
                    names.append(event.region.name)
                else:
                    names.append("N/A")

                attributesDict =  {}
                for key, value in vars(event).items(): #iterating through event attributes
                    if key == "time":
                        timestamps.append(value)
                    else:
                        attributesDict[key] = self.handleData(value)

                eventAttributes.append(attributesDict)

            #returns the events as a DataFrame
            eventsDF = pd.DataFrame({"Event": eventTypes, "Name": names, "Location": locations, "Timestamp": timestamps, "Attributes": eventAttributes})
            return eventsDF

    #returns a tuple containing definitions and events
    def read(self):
        with otf2.reader.open(self.dir_name) as trace:
            self.definitions = self.defToDF(trace)
            self.events = self.eventsToDF(trace)
        return (self.definitions, self.events)