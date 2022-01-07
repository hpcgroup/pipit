#import essential libraries
import numpy as np
import pandas as pd
from tqdm import tqdm
from collections import defaultdict



#add from_otf2 and from_nsight static methods
class TraceFrame:
    #instead of this, could also have a main class (one that would be used by nsight)
    #and a subclass of it that would include definitions (for all other formats)
    def __init__(self, definitions, events):
        #add error handling (Value Error) similar to hatchet -> add this to other files such as the readers as well
        self.definitions = definitions #None for Nsight
        self.events = events
        self.isNsight = self.definitions is None


    def summary(self, filteringDict = {"Name": "all", "Location ID": "all"}, summaryColumn = "Name"):
        #maybe there should be some restriction as to what the summary column can be (error handling in general has to be added)
        #is there a way to do this without two groupbys being used (would it make faster?)

        mask = pd.Series(np.ones((len(self.events),)), dtype = bool) if self.isNsight else self.events["Name"] != "N/A"
        for column, values in filteringDict.items():
            if isinstance(values, (list, tuple, set)):
                mask &= self.events[column].isin(values)
            elif (isinstance(values, str) or isinstance(values, int))and values != "all":
                mask &= self.events[column] == values

        filteredDF = self.events.loc[mask]

        observed = filteredDF[summaryColumn].dtype.name == "category"
        if self.isNsight:
            summaryDict = (filteredDF.groupby(summaryColumn, observed = observed)["End (ns)"].sum() - filteredDF.groupby(summaryColumn, observed = observed)
                           ["Start (ns)"].sum()).to_dict()
        else:
            summaryDict = (filteredDF.loc[filteredDF["Event"] == "Leave"].groupby(summaryColumn, observed = observed)["Timestamp (ns)"].sum()
                           - filteredDF.loc[filteredDF["Event"] == "Enter"].groupby(summaryColumn, observed = observed)["Timestamp (ns)"].sum()).to_dict()
            
        return summaryDict


    def eventTimes(self, filteringDict = {"Name": "all", "Location ID": "all"}):
        aggDict = defaultdict(lambda: [])

        mask = self.events["Name"] != "N/A"
        for column, values in filteringDict.items():
            if isinstance(values, (list, tuple, set)):
                mask &= self.events[column].isin(values)
            elif (isinstance(values, str) or isinstance(values, int))and values != "all":
                mask &= self.events[column] == values

        filteredDF = self.events.loc[mask]

        locIds = set(filteredDF["Location ID"])
        for locId in tqdm(locIds):
            locDF = filteredDF.loc[filteredDF["Location ID"] == locId]
            
            eventNames = set(locDF["Name"])
            for eventName in eventNames:
                currIndex = list(locDF.loc[locDF["Name"] == eventName].index)
                if len(currIndex) % 2 != 0:
                    currIndex.pop()

                aggDict[eventName].extend(self.events.iloc[currIndex[1::2]]["Timestamp (ns)"].values -
                                          self.events.iloc[currIndex[::2]]["Timestamp (ns)"].values)

        return aggDict


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

    
    #maybe rename to statsDict since it's not just stats for any iterables in general?
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
