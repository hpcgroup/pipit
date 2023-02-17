class QueryBuilder:
    def __init__(self, query, orderBy=None, limit=None, events=None):
        self.query = query

        if self.query.events is None:
            self.query.events = events

        self._orderBy = orderBy
        self._limit = limit
        self.events = events

    def where(self, field, operator, value):
        self.query = AndQuery(
            self.query,
            SimpleQuery(field, operator, value, self.events),
            self.events,
        )
        return self

    def orderBy(self, field, direction="asc"):
        self._orderBy = (field, direction)
        return self

    def limit(self, limitNum, limitStrategy="head"):
        self._limit = (limitNum, limitStrategy)
        return self

    def get(self):
        df = self.events[self.query._evaluate()]

        if self.orderBy is not None:
            df = df.sort_values(by=[self.orderBy.field])  # TODO: ascending/descending

        if self.limit is not None:
            df = df.head(n=self.limit.limitNum)  # TODO: limit strategy

        return df


class SimpleQuery:
    def __init__(self, field, operator, value, events=None):
        self.field = field
        self.operator = operator
        self.value = value
        self.events = events

    def __repr__(self):
        return str(self.field) + " " + str(self.operator) + " " + str(self.value)

    def _evaluate(self):
        """Returns boolean series which can be used to index events dataframe"""
        if self.operator == "==":
            return self.events[self.field] == self.value

        elif self.operator == "<":
            return self.events[self.field] < self.value

        elif self.operator == "<=":
            return self.events[self.field] <= self.value

        elif self.operator == ">":
            return self.events[self.field] > self.value

        elif self.operator == ">=":
            return self.events[self.field] >= self.value

        elif self.operator == "!=":
            return self.events[self.field] != self.value

        elif self.operator == "in":
            return self.events[self.field].isin(self.value)

        elif self.operator == "not-in":
            return -self.events[self.field].isin(self.value)

        elif self.operator == "between":
            return (self.events[self.field] >= self.value[0]) & (
                self.events[self.field] <= self.value[1]
            )


class OrQuery:
    def __init__(self, q1, q2, events):
        self.q1 = q1
        self.q2 = q2

        if q1.events is None:
            q1.events = events

        if q2.events is None:
            q2.events = events

        self.events = events

    def __repr__(self):
        return "(" + self.q1.__repr__() + ") || (" + self.q2.__repr__() + ")"

    def _evaluate(self):
        return self.q1._evaluate() | self.q2._evaluate()


class AndQuery:
    def __init__(self, q1, q2, events):
        self.q1 = q1

        if q1.events is None:
            q1.events = events

        if q2.events is None:
            q2.events = events

        self.q2 = q2
        self.events = events

    def __repr__(self):
        return "(" + self.q1.__repr__() + ") && (" + self.q2.__repr__() + ")"

    def _evaluate(self):
        return self.q1._evaluate() & self.q2._evaluate()
