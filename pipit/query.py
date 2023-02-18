class Where:
    def __init__(self, field=None, operator=None, value=None, query=None):
        self.field = field
        self.operator = operator
        self.value = value
        self.query = query

    def get_pandas_query(self):
        query = self.query

        if self.operator in ["==", "<", "<=", ">", ">=", "!="]:
            query = f"`{self.field}` {self.operator} {self.value.__repr__()}"
        elif self.operator == "in":
            query = f"`{self.field}`.isin({self.value.__repr__()})"
        elif self.operator == "not-in":
            query = f"-`{self.field}`.isin({self.value.__repr__()})"
        elif self.operator == "between":
            query = (
                f"(`{self.field}` >= {self.value[0].__repr__()})"
                f"& (`{self.field}` <= {self.value[1].__repr__()})"
            )

        return query

    def __and__(self, w2):
        return And(self, w2)

    def __or__(self, w2):
        return Or(self, w2)

    def __invert__(self):
        return Not(self)

    def __repr__(self):
        if self.query is not None:
            return f"Where {self.query}"
        else:
            return (
                f"Where {self.field.__repr__()} {self.operator} {self.value.__repr__()}"
            )


class And(Where):
    def __init__(self, *args):
        self.wheres = args

    def get_pandas_query(self):
        return " & ".join(f"({where.get_pandas_query()})" for where in self.wheres)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.wheres)


class Or(Where):
    def __init__(self, *args):
        self.wheres = args

    def get_pandas_query(self):
        return " | ".join(f"({where.get_pandas_query()})" for where in self.wheres)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.wheres)


class Not(Where):
    def __init__(self, where):
        self.where = where

    def get_pandas_query(self):
        return f"!({self.where.get_pandas_query()})"

    def __repr__(self):
        return f"Not ({self.where.__repr__()})"


class OrderBy:
    def __init__(self, field, direction):
        self.field = field
        self.direction = direction

    def __repr__(self):
        return f"OrderBy {self.field.__repr__()} {self.direction}"


class Limit:
    def __init__(self, num, strategy):
        self.num = num
        self.strategy = strategy

    def __repr__(self):
        return f"Limit {self.num} {self.strategy}"


class QueryBuilder:
    def __init__(self, trace=None):
        self._queries = []
        self.trace = trace

    def where(self, field, operator, value):
        self._queries.append(Where(field, operator, value))
        return self

    def __and__(self, q2):
        # Filter by wheres
        q1_wheres = [query for query in self._queries if isinstance(query, Where)]
        q2_wheres = [query for query in q2._queries if isinstance(query, Where)]

        # Concat wheres
        self._queries = q1_wheres + q2_wheres
        return self

    def __or__(self, q2):
        # Filter by wheres
        q1_wheres = [query for query in self._queries if isinstance(query, Where)]
        q2_wheres = [query for query in q2._queries if isinstance(query, Where)]

        # Or wheres
        self._queries = [Or(*(q1_wheres + q2_wheres))]
        return self

    def orderBy(self, field, direction="asc"):
        self._queries.append(OrderBy(field, direction))
        return self

    def limit(self, num, strategy="head"):
        self._queries.append(Limit(num, strategy))
        return self

    def get(self, trace=None):
        if trace is None:
            trace = self.trace

        df = trace.events
        orders_so_far = []

        for query in self._queries:
            if isinstance(query, Where):
                df = df.query(query.get_pandas_query())

            elif isinstance(query, OrderBy):
                orders_so_far.append(query)
                df = df.sort_values(
                    by=[x.field for x in orders_so_far],
                    ascending=[x.direction == "asc" for x in orders_so_far],
                )

            elif isinstance(query, Limit):
                df = df.head(query.num)

        return df

    def __repr__(self):
        return "Query " + self._queries.__repr__()
