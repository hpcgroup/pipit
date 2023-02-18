import pandas as pd

class Where:
    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self.value = value

    def __repr__(self):
        return str(self.field) + " " + str(self.operator) + " " + str(self.value)

    def __and__(self, where2):
        return And(self, where2)

    def __or__(self, where2):
        return Or(self, where2)

    def evaluate(self, events):
        if self.operator == "==":
            return events[self.field] == self.value
        elif self.operator == "<":
            return events[self.field] < self.value
        elif self.operator == "<=":
            return events[self.field] <= self.value
        elif self.operator == ">":
            return events[self.field] > self.value
        elif self.operator == ">=":
            return events[self.field] >= self.value
        elif self.operator == "!=":
            return events[self.field] != self.value
        elif self.operator == "in":
            return events[self.field].isin(self.value)
        elif self.operator == "not-in":
            return -events[self.field].isin(self.value)
        elif self.operator == "between":
            return (events[self.field] >= self.value[0]) & (
                events[self.field] <= self.value[1]
            )


class And:
    def __init__(self, where1, where2):
        self.where1 = where1
        self.where2 = where2

    def __and__(self, where2):
        return And(self, where2)

    def __or__(self, where2):
        return Or(self, where2)

    def __repr__(self):
        return f"({self.where1.__repr__()}) && ({self.where2.__repr__()})"

    def evaluate(self, events):
        return self.where1.evaluate(events) & self.where2.evaluate(events)


class Or:
    def __init__(self, where1, where2):
        self.where1 = where1
        self.where2 = where2

    def __and__(self, where2):
        return And(self, where2)

    def __or__(self, where2):
        return Or(self, where2)

    def __repr__(self):
        return f"({self.where1.__repr__()}) || ({self.where2.__repr__()})"

    def evaluate(self, events):
        return self.where1.evaluate(events) | self.where2.evaluate(events)


class Not:
    def __init__(self, where):
        self.where = where

    def __and__(self, where2):
        return And(self, where2)

    def __or__(self, where2):
        return Or(self, where2)

    def __repr__(self):
        return f"~({self.where.__repr__()})"

    def evaluate(self, events):
        return -self.where.evaluate(events)


class OrderBy:
    def __init__(self, field, direction):
        self.field = field
        self.direction = direction


class Limit:
    def __init__(self, num, strategy):
        self.num = num
        self.strategy = strategy


class QueryBuilder:
    def __init__(self, where=None, orderBy=[], limit=None, trace=None):
        self._where = where
        self._orderBy = orderBy
        self._limit = limit
        self.trace = trace

    def where(self, field, operator, value):
        tmp = Where(field, operator, value)
        self._where = And(self._where, tmp) if self._where is not None else tmp
        return self

    def __repr__(self):
        return self._where.__repr__() if self._where is not None else "querybuilder"

    def __and__(self, q):
        self._where = And(self._where, q._where)
        return self

    def __or__(self, q):
        self._where = Or(self._where, q._where)
        return self

    def orderBy(self, field, direction="asc"):
        self._orderBy.append((field, direction))
        return self

    def limit(self, num, strategy="head"):
        self._limit = Limit(num, strategy)
        return self

    def get(self, trace=None):
        df = pd.DataFrame()

        if trace is not None:
            df = trace.events[self._where.evaluate(trace.events)]

        if self.trace is not None:
            df = self.trace.events[self._where.evaluate(self.trace.events)]

        by = []
        ascending = []

        for orderBy in self._orderBy:
            by.append(orderBy[0])
            ascending.append(orderBy[1] == "asc")

        df = df.sort_values(by=by, ascending=ascending)


        return df
