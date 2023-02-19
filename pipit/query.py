class Query:
    """Base query class"""

    def apply(self, df, queries):
        """Given a DataFrame, apply the current query"""
        return df


class Where(Query):
    """Filters events based on field values, like `Name` or `Process`. Can operate on
    any field that exists in the `events` DataFrame. Supports basic comparisons
    (`==`, `<`, `>`, `<=`, `>=`, `!=`), as well as other operations, like `in`,
    `not-in`, and `between`. If an event does not meet the required condition,
    it will be filtered out.
    """

    def __init__(self, field=None, operator=None, value=None, query=None):
        """
        Parameters
        ----------
        field: str, optional
            The DataFrame field/column name to evaluate this query on

        operator: str, optional
            The comparison operator to use to evaluate this query.
            Can be any of:
            - `==`, `!=`
            - `<`, `<=`
            - `>`, `>=`
            - `in`, `not-in`
            - `between`

        value: optional
            The value to compare to. For `in` and `not-in` operations, this can be an
            list of any size. For `between`, this must be a list of size 2. For all
            other operators, must be a scalar value, like `"MPI_Init"` or `1.5e+8`.

        query: str, optional
            Instead of providing the `field`, `operator`, and `value`, you may provide
            a Pandas query string to evaluate and filter with.
            See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html. # noqa: E501
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.query = query

    def get_pandas_query(self):
        """
        Converts the query into a Pandas query string that can be fed into
        `DataFrame.query` for efficient evaluation.
        See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html.
        """
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

    def apply(self, df, queries):
        """Use `DataFrame.query` to evaluate the query and filter the DataFrame."""
        return df.query(self.get_pandas_query())


class And(Where):
    """Combines multiple Where queries with a logical `AND`, such that all of the
    conditions must be met. If an event does not meet all conditions, it will be
    filtered out."""

    def __init__(self, *args):
        self.wheres = args

    def get_pandas_query(self):
        return " & ".join(f"({where.get_pandas_query()})" for where in self.wheres)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.wheres)


class Or(Where):
    """Combines multiple Where queries with a logical `OR`, such that any of the
    conditions must be met. If an event does not meet any of the conditions, it will
    be filtered out."""

    def __init__(self, *args):
        self.wheres = args

    def get_pandas_query(self):
        return " | ".join(f"({where.get_pandas_query()})" for where in self.wheres)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.wheres)


class Not(Where):
    """Inverts a Where query with a logical `NOT`, such that the condition must not be
    met."""

    def __init__(self, where):
        self.where = where

    def get_pandas_query(self):
        return f"!({self.where.get_pandas_query()})"

    def __repr__(self):
        return f"Not ({self.where.__repr__()})"


class OrderBy(Query):
    """Sorts events by any field."""

    def __init__(self, field, direction="asc"):
        """
        Parameters
        ----------
        field: str
            Field to sort by. Can be any column in the DataFrame.

        direction: str
            "asc" for ascending order, "desc" for descending order
        """
        self.field = field
        self.direction = direction

    def __repr__(self):
        return f"OrderBy {self.field.__repr__()} {self.direction}"

    def apply(self, df, queries):
        # Combine the OrderBy queries so far
        orders_so_far = []

        for query in queries:
            if isinstance(query, OrderBy):
                orders_so_far.append(query)

            if query == self:
                break

        # Apply the queries so far
        return df.sort_values(
            by=[x.field for x in orders_so_far],
            ascending=[x.direction == "asc" for x in orders_so_far],
        )


class Limit(Query):
    """Limits the number of events."""

    def __init__(self, num, strategy="head"):
        """
        Parameters
        ----------
        num: int
            Number of items to limit to

        strategy: int
            How to downsample the events: `head`, `tail`, or `random`.
        """
        self.num = num
        self.strategy = strategy

    def __repr__(self):
        return f"Limit {self.num} {self.strategy}"

    def apply(self, df, queries):
        return df.head(self.num)


class QueryBuilder:
    """Used to contain and chain multiple queries to be applied to a `Trace`."""

    def __init__(self, trace=None, *queries):
        """
        Parameters:
        ----------
        trace: pipit.Trace, optional
            The default `Trace` instance to apply queries to

        queries: list[pipit.query.Query], optional
            List of queries to import to this QueryBuilder
        """
        self.trace = trace
        self.queries = list(queries)

    def where(self, field, operator, value):
        """Adds a Where query, used to filter events by field values."""
        self.queries.append(Where(field, operator, value))
        return self

    def orderBy(self, field, direction="asc"):
        """Adds an OrderBy query, used to sort the events."""
        self.queries.append(OrderBy(field, direction))
        return self

    def limit(self, num, strategy="head"):
        """Adds a Limit query, used to downsample the number of events."""
        self.queries.append(Limit(num, strategy))
        return self

    def __and__(self, q2):
        """Override the default behavior for `&` to concatenate two Where queries"""
        # Filter by Where queries
        # Behavior for OrderBy and Limit queries is undefined
        wheres = [query for query in self.queries if isinstance(query, Where)]
        wheres2 = [query for query in q2.queries if isinstance(query, Where)]

        # Concatenate Where queries into one large array
        self.queries = wheres + wheres2
        return self

    def __or__(self, q2):
        """Override the default behavior for `|` to combine into an Or query"""
        # Filter by Where queries
        # Behavior for OrderBy and Limit queries is undefined
        wheres = [query for query in self.queries if isinstance(query, Where)]
        wheres2 = [query for query in q2.queries if isinstance(query, Where)]

        # Combine Where queries under an Or query
        self.queries = [Or(*(wheres + wheres2))]
        return self

    def __invert__(self):
        """Override the default behavior for `~` to combine turn into a Not query"""
        # Filter by Where queries
        # Behavior for OrderBy and Limit queries is undefined
        wheres = [query for query in self.queries if isinstance(query, Where)]

        # Combine Where queries under a Not query
        self.queries = [Not(*wheres)]
        return self

    def get(self, trace=None):
        """Apply all queries to a `Trace` instance and return the filtered/sorted events
        DataFrame. If `trace` parameter is not provided, apply queries to default
        `Trace` passed in during initialization of this `QueryBuilder`.
        """
        if trace is None:
            trace = self.trace

        return trace.query(*self.queries)

    def __repr__(self):
        return "QueryBuilder " + self.queries.__repr__()
