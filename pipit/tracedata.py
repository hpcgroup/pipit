#import essential libraries
import math
import numpy as np
import pandas as pd
from tqdm import tqdm
from collections import defaultdict



#add from_otf2 and from_nsight static methods
class TraceData:
    def __init__(self, definitions, events):
        #add error handling (Value Error) similar to hatchet -> add this to other files such as the readers as well
        self.definitions = definitions #None for Nsight
        self.events = events
        self.isNsight = self.definitions is None


    #function to decrease code redundancy (used in other functions)
    def maskFromFilter(self, filteringDict):
        #all the functions using maskFromFilter filter the N/A events out by default
        mask = pd.Series(np.ones((len(self.events),)), dtype = bool) if self.isNsight else self.events["Name"] != "N/A"
        for column, values in filteringDict.items():
            if isinstance(values, (list, tuple, set)):
                mask &= self.events[column].isin(values)
            elif (isinstance(values, str) or isinstance(values, int)) and values != "all":
                mask &= self.events[column] == values
        return mask


    def summary(self, filteringDict = {"Name": "all", "Location ID": "all"}, summaryColumn = "Name"):
        #maybe there should be some restriction as to what the summary column can be (error handling in general has to be added)

        filteredDF = self.events.loc[self.maskFromFilter(filteringDict)]

        observed = filteredDF[summaryColumn].dtype.name == "category"
        summaryDict = (filteredDF.loc[filteredDF["Event"] == "Leave"].groupby(summaryColumn, observed = observed)["Timestamp (ns)"].sum()
                       - filteredDF.loc[filteredDF["Event"] == "Enter"].groupby(summaryColumn, observed = observed)["Timestamp (ns)"].sum()).to_dict()
            
        return summaryDict


    def eventTimes(self, filteringDict = {"Name": "all", "Location ID": "all"}):
        aggDict = defaultdict(lambda: [])
        timesArray = np.array(self.events["Timestamp (ns)"].values)
        filteredDF = self.events.loc[self.maskFromFilter(filteringDict)]

        locIds = set(filteredDF["Location ID"])
        for locId in tqdm(locIds):
            locDF = filteredDF.loc[filteredDF["Location ID"] == locId]
            
            eventNames = set(locDF["Name"])
            for eventName in eventNames:
                currIndex = list(locDF.loc[locDF["Name"] == eventName].index)
                if len(currIndex) % 2 != 0:
                    currIndex.pop() #handles anomalies

                #numpy indexing is much more efficient than pandas
                aggDict[eventName].extend((timesArray[currIndex[1::2]] - timesArray[currIndex[::2]]).tolist())

        return aggDict


    #reduce code redundancy by using the same function for matchRows and eventTimes?
    def matchRows(self, filteringDict = {"Name": "all", "Location ID": "all"}):
        #nsight rows are already matched
        if not self.isNsight:
            matchingIDs = np.full((len(self.events),), -1) #unmatched rows will have a matching id of -1
            filteredDF = self.events.loc[self.maskFromFilter(filteringDict)]

            i = 0
            locIds = set(filteredDF["Location ID"])
            for locId in tqdm(locIds):
                locDF = filteredDF.loc[filteredDF["Location ID"] == locId]
                
                eventNames = set(locDF["Name"])
                for eventName in eventNames:
                    currIndex = list(locDF.loc[locDF["Name"] == eventName].index)

                    #numpy indexing is much more efficient than pandas
                    matchingIDs[currIndex[::2]] = np.arange(i, i + len(currIndex[::2]))
                    matchingIDs[currIndex[1::2]] = np.arange(i, i + len(currIndex[1::2]))
                    i += math.ceil(len(currIndex) / 2) #accounts for anomalies like an odd number of rows (unmatched enter, etc)

            self.events["Matching ID"] = matchingIDs


    #clarify which axis is the sender/receiver
    def p2p(self, mirrored = False, commType = "bytes"):
        #figure out what nsight data would look like for an mpi program
        if not self.isNsight:
            ranks = set(self.events["Location Group ID"])
            comms = np.zeros(shape = (len(ranks), len(ranks)))

            sends = self.events.loc[self.events["Event"].isin(["MpiSend", "MpiIsend"])]
            x = sends["Location Group ID"].to_list()
            y = sends["Attributes"].apply(lambda attrDict: attrDict["receiver"]).to_list()
            
            if commType == "bytes":
                vol = sends["Attributes"].apply(lambda attrDict: attrDict["msg_length"]).to_list()
            elif commType == "counts":
                vol = np.full(len(sends), 1)
                
            for i in range(len(x)):
                comms[x[i], y[i]] += vol[i]
            if mirrored == True:
                for i in range(len(x)):
                    comms[y[i], x[i]] += vol[i]
                
            return comms


    @staticmethod
    def sortDict(dictToSort, sortBy = "values", reverse = False):
        if sortBy == "keys":
            keyInt = 0
        elif sortBy == "values":
            keyInt = True
        return dict(sorted(dictToSort.items(), key = lambda dictTuple: dictTuple[keyInt], reverse = reverse))

    
    #maybe rename to statsDict since it's not just stats for any iterable in general?
    @staticmethod
    def stats(timesDict, operation):
        keys, times = list(timesDict.keys()), list(timesDict.values())
        if operation == "mean":
            return np.mean(times)
        elif operation == "median":
            return np.median(times)
        elif operation == "std-dev":
            return np.std(times)
        elif operation == "zscores":
            meanTime = np.mean(times)
            stdDeviation = np.std(times)
            return dict(zip(keys, (times - meanTime) / stdDeviation))
        elif operation == "minmaxscaling":
            minTime = np.min(times)
            maxTime = np.max(times)
            return dict(zip(keys, (times - minTime) / (maxTime - minTime)))


    @staticmethod
    def from_otf2(dir_name):
        from .readers.otf2_reader import OTF2Reader
        return OTF2Reader(dir_name).read()
        
        
    @staticmethod
    def from_nsight(nvtx_dir_name, cuda_dir_name, gpu_dir_name):
        from .readers.nsight_reader import NsightReader
        return NsightReader(nvtx_dir_name, cuda_dir_name, gpu_dir_name).read()
