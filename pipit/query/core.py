# stripped down version of initial.py
# these include basic SQL commands

class Event:
    def __init__(self):
        self.timestamp = 0
        self.eventType = "Instant"
        self.name = "Event"
        self.process = 0
        self.thread = 0
        self.attributes = dict()

# maybe this can be modeled after HoloViews Dataset: https://holoviews.org/getting_started/Tabular_Datasets.html
# in hv.DataSet, there are kdims (which are independent variables) and vdims (which are dependent variables)
class Dataset:
    def where(self, condition): # filter
        pass

    def sum(self, field):   # aggregate
        pass

    def count(self, field): # aggregate
        pass

    def mean(self, field):  # aggregate
        pass

    def min(self, field):   # aggregate
        pass

    def max(self, field):   # aggregate
        pass

    def group_by(self, field):  # groupby
        pass

    def insert(self, event):    # insert
        pass
