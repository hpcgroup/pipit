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
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __invert__(self):
        return Not(self)

    def __repr__(self):
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
        value = self.value

        # Parse value into float, if needed
        if self.field and "time" in self.field.lower():
            value = parse_time(self.value)

        # Get boolean mask using pd.DataFrame.eval
        if self.expr:
            result = trace.events.eval(self.expr)

        # Get boolean mask using pd.Series comparison
        elif self.operator == "==":
            result = trace.events[self.field] == value

        elif self.operator == "!=":
            result = trace.events[self.field] != value

        elif self.operator == "<":
            result = trace.events[self.field] < value

        elif self.operator == "<=":
            result = trace.events[self.field] <= value

        elif self.operator == ">":
            result = trace.events[self.field] > value

        elif self.operator == ">=":
            result = trace.events[self.field] >= value

        elif self.operator == "in":
            result = trace.events[self.field].isin(value)

        elif self.operator == "not-in":
            result = ~trace.events[self.field].isin(value)

        elif self.operator == "between":
            start, end = value

            if self.field != "Timestamp (ns)":
                result = (trace.events[self.field] >= start) & (
                    trace.events[self.field] <= end
                )
            else:
                # Special case if field is timestamp
                trace._match_events()

                # This ensures that if any part of a function occurs in a time range,
                # then both the Enter and Leave events are included in the filter
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
