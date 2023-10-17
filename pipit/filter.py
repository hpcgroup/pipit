from .util import parse_time


class Filter:
    """
    A filter that can be used to select a subset of events from a Trace instance
    based on a condition on a field, like `Name == "MPI_Init"` or `Process > 5`.

    Filter instances can be modified with the AND, OR, and NOT logical operators.
    """

    def __init__(
        self,
        field=None,
        operator=None,
        value=None,
        expr=None,
    ):
        """
        Args:
            field (str, optional): DataFrame column to filter on.

            operator (str, optional): The comparison operator to use for filtering.
                Available operators are `<`, `<=`, `==`, `>=`, `>`, `!=`, `in`, `not-in`,
                and `between`.

            value (optional): The value to compare against when filtering. If operator
                is `in` or `not-in`, this must be a list of values. If operator is
                `between`, this must be a list of 2 elements, containing the start
                and end values.

            expr (str, optional): Pandas expression that can be provided as an
                alternative to the field, operator, and value parameters. When evaluated
                with `pandas.DataFrame.eval`, it should return a boolean mask indicating
                whether each event should be included in the filtered Trace.
                See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.eval.html. # noqa: E501
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.expr = expr

    def __and__(self, other):
        """Returns a new Filter instance that combines this filter and another filter
        with the logical AND operator.
        """
        return And(self, other)

    def __or__(self, other):
        """Returns a new Filter instance that combines this filter and another filter
        with the logical OR operator.
        """
        return Or(self, other)

    def __invert__(self):
        """Returns a new Filter instance that negates this filter with the logical NOT
        operator.
        """
        return Not(self)

    def __repr__(self):
        """Returns a string representation of this filter."""
        if self.expr is not None:
            return f"Filter {self.expr.__repr__()}"

        else:
            return (
                f"Filter {self.field.__repr__()} "
                + f"{self.operator} {self.value.__repr__()}"
            )

    def _eval(self, trace):
        """Evaluatea this filter on a Trace.

        Returns:
            pd.Series: Boolean mask that indicates whether each event should be included
                in the filtered Trace.
        """
        # If an expression is provided, evaluate it using pd.DataFrame.eval
        if self.expr is not None:
            return trace.events.eval(self.expr)

        # Otherwise, evaluate the filter using speficied field, operator, and value
        field, operator, value = self.field, self.operator, self.value

        # Convert value to float if filtering on a time field
        if field and "time" in field.lower():
            value = parse_time(value)

        # Evaluate the filter
        # If field is not Timestamp, then evaluation is straightforward
        if field != "Timestamp (ns)":
            if operator == "==":
                result = trace.events[field] == value
            elif operator == "!=":
                result = trace.events[field] != value
            elif operator == "<":
                result = trace.events[field] < value
            elif operator == "<=":
                result = trace.events[field] <= value
            elif operator == ">":
                result = trace.events[field] > value
            elif operator == ">=":
                result = trace.events[field] >= value
            elif operator == "in":
                result = trace.events[field].isin(value)
            elif operator == "not-in":
                result = ~trace.events[field].isin(value)
            elif operator == "between":
                result = (trace.events[field] >= value[0]) & (
                    trace.events[field] <= value[1]
                )
            else:
                raise ValueError(
                    f'Invalid comparison operator "{operator}" for field "{field}"'
                )
        else:
            # We need to ensure that if any of function duration is in the
            # time range, then both Enter and Leave events are included in the mask
            trace._match_events()

            # Extract start and end timestamps if operator is <, <=, >, >=, or between
            start, end = float("-inf"), float("inf")

            if operator == "<" or operator == "<=":
                end = value
            elif operator == ">" or operator == ">=":
                start = value
            elif operator == "between":
                start, end = value
            else:
                raise ValueError(
                    f'Invalid comparison operator "{operator}" for field "{field}"'
                )

            # Handle each event type separately
            result = (
                (
                    (trace.events["Event Type"] == "Instant")
                    & (trace.events["Timestamp (ns)"] >= start)
                    & (trace.events["Timestamp (ns)"] <= end)
                )
                | (
                    (trace.events["Event Type"] == "Enter")
                    & (trace.events["_matching_timestamp"] >= start)
                    & (trace.events["Timestamp (ns)"] <= end)
                )
                | (
                    (trace.events["Event Type"] == "Leave")
                    & (trace.events["Timestamp (ns)"] >= start)
                    & (trace.events["_matching_timestamp"] <= end)
                )
            )

        return result


class And(Filter):
    """Combines multiple Filter objects with a logical AND, such that all of the
    filters must be met."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _eval(self, trace):
        # Evaluate the first filter on the trace
        results = self.filters[0]._eval(trace)

        # Evaluate the rest of the filters, one at a time,
        # and AND the result each time
        for i in range(1, len(self.filters)):
            results = results & self.filters[i]._eval(trace)

        return results

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.filters)


class Or(Filter):
    """Combines multiple Filter objects with a logical OR, such that any of the
    filters must be met."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _eval(self, trace):
        # Evaluate the first filter on the trace
        results = self.filters[0]._eval(trace)

        # Evaluate the rest of the filters, one at a time,
        # and OR the result each time
        for i in range(1, len(self.filters)):
            results = results | self.filters[i]._eval(trace)

        return results

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.filters)


class Not(Filter):
    """Inverts Filter object with a logical NOT, such that the filter must not be
    met."""

    def __init__(self, filter):
        super().__init__()
        self.filter = filter

    def _eval(self, trace):
        return ~self.filter._eval(trace)

    def __repr__(self):
        return f"Not ({self.filter.__repr__()})"
