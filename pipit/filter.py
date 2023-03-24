import pipit
from .util import parse_time
import pandas as pd


class Filter:
    """Represents a conditional filter along a field, like "Name" or "Process".
    Can operate on any field that currently exists in the events DataFrame. Supports
    basic operators, like "<", "<=", "==", ">=", ">", "!=", as well as other
    convenient operators like "in", "not-in", and "between".

    Filter instances can be shared and reused across multiple Traces. They can also
    be combined with logical AND or OR, and negated with a logical NOT. When a Filter
    is applied to a Trace, a new Trace instance is created containing a view of the
    events DataFrame of the original Trace. All of Pipit's analysis and plotting
    functions can be applied to the filtered Trace.
    """

    # TODO: Add "func" arg so user can provide a lambda function to filter with
    def __init__(
        self,
        field=None,
        operator=None,
        value=None,
        expr=None,
        validate="keep",
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

            validate (optional): How to validate the Trace. Can be "keep", "remove", or False.
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.expr = expr
        self.validate = validate

    def _get_pandas_expr(self):
        """
        Converts the filter into a Pandas expression that can be fed into
        DataFrame.query for vectorized evaluation.
        See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html.
        """
        expr = self.expr

        field = self.field
        operator = self.operator
        value = self.value

        # Parse value if timestamp
        if field and "time" in field.lower():
            value = parse_time(value)

        # Convert to expr
        if operator in ["==", "<", "<=", ">", ">=", "!="]:
            expr = f"`{field}` {operator} {value.__repr__()}"
        elif operator == "in":
            expr = f"`{field}`.isin({value.__repr__()})"
        elif operator == "not-in":
            expr = f"-`{field}`.isin({value.__repr__()})"
        elif operator == "between":
            field1 = field
            field2 = field

            if field == "Timestamp (ns)":
                field2 = "_matching_timestamp"

            expr = (
                f"(`{field2}` >= {value[0].__repr__()}) "
                f"& (`{field1}` <= {value[1].__repr__()})"
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
        trace._match_events()

        # Filter events using either DataFrame.query or DataFrame.apply
        events = trace.events.query(self._get_pandas_expr())

        # Ensure returned trace is valid
        # Ensure that matches of filtered events are always included
        if self.validate == "keep":
            matching_rows = trace.events.loc[
                events["_matching_event"].dropna().tolist()
            ]
            events = pd.concat([events, matching_rows]).sort_index()
            events = events[~events.index.duplicated(keep="first")]

        # Remove events whose matching events did not make filter
        if self.validate == "remove":
            events = events[
                (~(events["Event Type"].isin(["Enter", "Leave"])))
                | (events["_matching_event"].isin(events.index))
            ]

        # TODO: filter CCT?

        return pipit.Trace(trace.definitions, events)


class And(Filter):
    """Combines multiple Filter objects with a logical AND, such that all of the
    filters must be met."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args
        self.validate = args[len(args) - 1].validate

    def _get_pandas_expr(self):
        # pandas query expression that combines filters with AND
        return " & ".join(f"({filter._get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.filters)


class Or(Filter):
    """Combines multiple Filter objects with a logical OR, such that any of the
    filters must be met."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args
        self.validate = args[len(args) - 1].validate

    def _get_pandas_expr(self):
        # pandas query expression that combines filters with OR
        return " | ".join(f"({filter._get_pandas_expr()})" for filter in self.filters)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.filters)


class Not(Filter):
    """Inverts Filter object with a logical NOT, such that the filter must not be
    met."""

    def __init__(self, filter):
        super().__init__()
        self.filter = filter
        self.validate = filter.validate

    def _get_pandas_expr(self):
        # pandas query expression that negates filters
        return f"~({self.filter._get_pandas_expr()})"

    def __repr__(self):
        return f"Not ({self.filter.__repr__()})"
