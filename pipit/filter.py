import pipit


class Filter:
    """Represents a selection along a field, like "Name" or "Process". Can operate
    on any field that currently exists in the events DataFrame. Supports
    basic comparisons, like "<", "<=", "==", ">=", ">", "!=", as well as
    other operations, like "in", "not-in", and "between".
    """

    def __init__(
        self,
        field=None,
        operator=None,
        value=None,
        expr=None,
        func=None,
        keep_invalid=False,
    ):
        """
        Args:
            field (str, optional): The DataFrame field/column name to select by.

            operator (str, optional): The comparison operator to use to evaluate
                the selection. Allowed operators:
                "<", "<=", "==", ">=", ">", "!=", "in", "not-in", "between"

            value (optional): The value to compare to. For "in" and "not-in"
                operations, this can be a list of any size. For "between", this
                must be a list or tuple of 2 elements. For all other operators,
                must be a scalar value, like "MPI_Init" or "1.5e+5"

            pandas_expr (str, optional): Instead of providing the field,
                operator, and value, you may provide a Pandas query expression
                to select with.
                See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html. # noqa: E501

            func (callable[row], optional): Instead of providing the field/
                operator/value, or providing a Pandas query expression, you
                may provide a function that is applied to each row of the
                DataFrame, that returns True or False. This uses the
                DataFrame.apply function, which is not a vectorized operation,
                resulting in a significantly slower runtime.

            keep_invalid (bool, optional): Whether to keep Enter/Leave events whose
                matching event did not make the selection. Defaults to False.
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.expr = expr
        self.func = func
        self.keep_invalid = keep_invalid

    def _get_pandas_expr(self):
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
                f"Filter {self.field.__repr__()} "
                + f"{self.operator} {self.value.__repr__()}"
            )

    def _apply(self, trace):
        # Filter events using either DataFrame.query or DataFrame.apply
        if self.func is None:
            events = trace.events.query(self._get_pandas_expr())
        else:
            events = trace.events[trace.events.apply(self.func, axis=1)]

        # Remove invalid events
        if not self.keep_invalid:
            events = events[events["_matching_event"].isin(events.index)]

        # TODO: filter cct?

        return pipit.Trace(trace.definitions, events)


class And(Filter):
    """Combines multiple `Filter` objects with a logical `AND`, such that all of the
    conditions must be met. If an event does not meet all conditions, it will be
    filtered out."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _get_pandas_expr(self):
        return " & ".join(f"({filter._get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.filters)


class Or(Filter):
    """Combines multiple `Filter` objects with a logical `OR`, such that any of the
    conditions must be met. If an event does not meet any of the conditions, it will
    be filtered out."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _get_pandas_expr(self):
        return " | ".join(f"({filter._get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.filters)


class Not(Filter):
    """Inverts a `Filter` object with a logical `NOT`, such that the condition must not
    be met."""

    def __init__(self, filter):
        super().__init__()
        self.filter = filter

    def _get_pandas_expr(self):
        return f"!({self.filter._get_pandas_expr()})"

    def __repr__(self):
        return f"Not ({self.filter.__repr__()})"
