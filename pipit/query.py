class Query:
    """Base query class. A `Query` is anything that, when applied to a `DataFrame`,
    returns a subset of that DataFrame (without necessarily maintaining order).
    Queries provide a nice abstraction over native Pandas `DataFrame` methods like
    `loc` and `sort_values`."""

    def apply(self, df, queries=[]):
        """Given a DataFrame, apply the current query"""
        return df


class Select(Query):
    """Used to select which columns to include."""

    DEFAULTS = [
        "Timestamp (ns)",
        "Event Type",
        "Name",
        "Thread",
        "Process",
        "Attributes",
    ]

    def __init__(self, *args):
        self.columns = list(args)

    def __repr__(self):
        return f"Select {self.columns.__repr__()}"

    def apply(self, df, queries=[]):
        if "all" in self.columns:
            return df

        raw_columns = set()

        for column in self.columns:
            if column == "defaults":
                for default in self.DEFAULTS:
                    raw_columns.add(default)
            else:
                raw_columns.add(column)

        return df.loc[:, df.columns.isin(list(raw_columns))]


class Exclude(Query):
    """Used to select which columns to exclude."""

    def __init__(self, *args):
        self.columns = list(args)

    def __repr__(self):
        return f"Exclude {self.columns.__repr__()}"

    def apply(self, df, queries=[]):
        return df.loc[:, ~df.columns.isin(self.columns)]


class Filter(Query):
    """Filters events based on field values, like `Name` or `Process`. Can operate on
    any field that exists in the `events` DataFrame. Supports basic comparisons
    (`==`, `<`, `>`, `<=`, `>=`, `!=`), as well as other operations, like `in`,
    `not-in`, and `between`. If an event does not meet the required condition,
    it will be filtered out.
    """

    def __init__(self, field=None, operator=None, value=None, expr=None, func=None):
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

        expr: str, optional
            Instead of providing the `field`, `operator`, and `value`, you may provide
            a Pandas expression to evaluate and filter with.
            See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html. # noqa: E501

        func: callable[row], optional
            Instead of providing the `field`, `operator`, and `value`, or providing
            a Pandas query string, you may provide a function that is applied to each
            row of the DataFrame, that returns `True` or `False`. This uses the
            `DataFrame.apply` function, which is not a vectorized operation, resulting
            in a significantly slower runtime.
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.expr = expr
        self.func = func

    def get_pandas_expr(self):
        """
        Converts the query into a Pandas expression that can be fed into
        `DataFrame.query` for efficient evaluation.
        See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html.
        """
        expr = self.expr

        if self.operator in ["==", "<", "<=", ">", ">=", "!="]:
            expr = f"`{self.field}` {self.operator} {self.value.__repr__()}"
        elif self.operator == "in":
            expr = f"`{self.field}`.isin({self.value.__repr__()})"
        elif self.operator == "not-in":
            expr = f"-`{self.field}`.isin({self.value.__repr__()})"
        elif self.operator == "between":
            expr = (
                f"(`{self.field}` >= {self.value[0].__repr__()})"
                f"& (`{self.field}` <= {self.value[1].__repr__()})"
            )

        return expr

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __invert__(self):
        return Not(self)

    def __repr__(self):
        if self.expr is not None:
            return f"Filter {self.expr.__repr__()}"

        elif self.func is not None:
            return f"Filter {self.func.__repr__()}"

        else:
            return (
                f"Filter {self.field.__repr__()}"
                + f"{self.operator} {self.value.__repr__()}"
            )

    def apply(self, df, queries=[]):
        """
        Use either `DataFrame.query` or `DataFrame.apply` to evaluate the query
        and filter the DataFrame.
        """
        if self.func is None:
            return df.query(self.get_pandas_expr())
        else:
            return df[df.apply(self.func, axis=1)]


class And(Filter):
    """Combines multiple `Filter` queries with a logical `AND`, such that all of the
    conditions must be met. If an event does not meet all conditions, it will be
    filtered out."""

    def __init__(self, *args):
        self.filters = args

    def get_pandas_expr(self):
        return " & ".join(f"({filter.get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.filters)


class Or(Filter):
    """Combines multiple `Filter` queries with a logical `OR`, such that any of the
    conditions must be met. If an event does not meet any of the conditions, it will
    be filtered out."""

    def __init__(self, *args):
        self.filters = args

    def get_pandas_expr(self):
        return " | ".join(f"({filter.get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.filters)


class Not(Filter):
    """Inverts a `Filter` query with a logical `NOT`, such that the condition must not
    be met."""

    def __init__(self, filter):
        self.filter = filter

    def get_pandas_expr(self):
        return f"!({self.filter.get_pandas_expr()})"

    def __repr__(self):
        return f"Not ({self.filter.__repr__()})"


class Sort(Query):
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
        return f"Sort {self.field.__repr__()} {self.direction}"

    def apply(self, df, queries=[]):
        # Combine the Sort queries so far
        sorts_so_far = []

        for query in queries:
            if isinstance(query, Sort):
                sorts_so_far.append(query)

            if query == self:
                break

        # Apply the queries so far
        return df.sort_values(
            by=[x.field for x in sorts_so_far],
            ascending=[x.direction == "asc" for x in sorts_so_far],
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

    def apply(self, df, queries=[]):
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

    def select(self, *args, **kwargs):
        """Adds a Select query, used to select certain columns."""
        self.queries.append(Select(*args, **kwargs))
        return self

    def exclude(self, *args, **kwargs):
        """Adds a Select query, used to select certain columns."""
        self.queries.append(Exclude(*args, **kwargs))
        return self

    def filter(self, *args, **kwargs):
        """Adds a Filter query, used to filter events by field values."""
        self.queries.append(Filter(*args, **kwargs))
        return self

    def sort(self, *args, **kwargs):
        """Adds a Sort query, used to sort the events."""
        self.queries.append(Sort(*args, **kwargs))
        return self

    def limit(self, *args, **kwargs):
        """Adds a Limit query, used to downsample the number of events."""
        self.queries.append(Limit(*args, **kwargs))
        return self

    def __and__(self, other):
        """Override the default behavior for `&` to concatenate all Filter queries"""
        # Filter by Filter queries
        # Behavior for Sort and Limit queries is undefined
        filters = [query for query in self.queries if isinstance(query, Filter)]
        filters2 = [query for query in other.queries if isinstance(query, Filter)]

        # Concatenate Filter queries into one large list
        self.queries = filters + filters2
        return self

    def __or__(self, other):
        """Override the default behavior for `|` to combine into an Or query"""
        # Filter by Filter queries
        # Behavior for Sort and Limit queries is undefined
        filters = [query for query in self.queries if isinstance(query, Filter)]
        filters2 = [query for query in other.queries if isinstance(query, Filter)]

        # Combine Filter queries under an Or query
        self.queries = [Or(*(filters + filters2))]
        return self

    def __invert__(self):
        """Override the default behavior for `~` to combine turn into a Not query"""
        # Filter by Filter queries
        # Behavior for Sort and Limit queries is undefined
        filters = [query for query in self.queries if isinstance(query, Filter)]

        # Combine Filter queries under a Not query
        self.queries = [Not(*filters)]
        return self

    def apply(self, trace=None):
        """Apply all queries to a `Trace` instance and return the filtered/sorted events
        DataFrame. If `trace` parameter is not provided, apply queries to default
        `Trace` passed in during initialization of this `QueryBuilder`.
        """
        if trace is None:
            trace = self.trace

        return trace.query(*self.queries)

    def __repr__(self):
        return "QueryBuilder " + self.queries.__repr__()
