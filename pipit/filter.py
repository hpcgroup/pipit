import pipit


class Filter:
    """Represents a conditional filter along a field, like "Name" or "Process".
    Can operate on any field that currently exists in the events DataFrame. Supports
    basic operators, like "<", "<=", "==", ">=", ">", "!=", as well as other
    convenient operators like "in", "not-in", and "between".

    Filter instances can be shared and reused across multiple Traces. They can also
    be combined with logical AND or OR, and negated with a logical NOT (as long as
    they do not use the func argument). When a Filter is applied to a Trace, a new Trace
    instance is created containing a view of the events DataFrame of the original Trace.
    All of Pipit's analysis and plotting functions can be applied to the filtered Trace.
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
            field (str, optional): The DataFrame field/column name to filter along.

            operator (str, optional): The comparison operator used to evaluate
                the filter condition. Allowed operators:
                "<", "<=", "==", ">=", ">", "!=", "in", "not-in", "between"

            value (optional): The value to compare to. For "in" and "not-in"
                operators, this can be a list of any size. For "between", this
                must be a list or tuple of 2 elements, containing the lower and upper
                bound. For all other operators, must be a scalar value, like "MPI_Init"
                or 1.5e+5.

            expr (str, optional): Instead of providing the field, operator, and value,
                you may provide a Pandas query expression to filter with.
                See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html. # noqa: E501

            func (callable[row], optional): Instead of providing any of the previous
                arguments, you may provide a function that returns True or False
                for each row of the DataFrame. This uses DataFrame.apply, which is
                not a vectorized operation, and may result in a significantly
                slower runtime than using the previous arguments. Note that &, |, and
                ~ will NOT work for Filter instances with the func argument.

            keep_invalid (bool, optional): Whether to keep Enter/Leave events whose
                matching event did not make the filter. If this is set to True,
                the filter may produce an invalid Trace object. Defaults to False.
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.expr = expr
        self.func = func
        self.keep_invalid = keep_invalid

    def _get_pandas_expr(self):
        """
        Converts the filter into a Pandas expression that can be fed into
        DataFrame.query for vectorized evaluation.
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
        """Apply this Filter to a Trace.

        Args:
            trace (pipit.Trace): Trace instance being filtered

        Returns:
            pipit.Trace: new Trace instance containing filtered events DataFrame
        """
        # Filter events using either DataFrame.query or DataFrame.apply
        if self.func is None:
            events = trace.events.query(self._get_pandas_expr())
        else:
            events = trace.events[trace.events.apply(self.func, axis=1)]

        # Remove events whose matching events did not make filter
        # Ensures that returned Trace is valid
        if not self.keep_invalid:
            events = events[events["_matching_event"].isin(events.index)]

        # TODO: filter CCT?

        return pipit.Trace(trace.definitions, events)


class And(Filter):
    """Combines multiple Filter objects with a logical AND, such that all of the
    filters must be met. Does NOT work for Filter instances with func argument."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _get_pandas_expr(self):
        # pandas query expression that combines filters with AND
        return " & ".join(f"({filter._get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.filters)


class Or(Filter):
    """Combines multiple Filter objects with a logical OR, such that any of the
    filters must be met. Does NOT work for Filter instances with func argument."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _get_pandas_expr(self):
        # pandas query expression that combines filters with OR
        return " | ".join(f"({filter._get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.filters)


class Not(Filter):
    """Inverts Filter object with a logical NOT, such that the filter must not be
    met. Does NOT work for Filter instances with func argument."""

    def __init__(self, filter):
        super().__init__()
        self.filter = filter

    def _get_pandas_expr(self):
        # pandas query expression that negates filters
        return f"!({self.filter._get_pandas_expr()})"

    def __repr__(self):
        return f"Not ({self.filter.__repr__()})"
