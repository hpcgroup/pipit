from .util import parse_time
import pandas as pd
from pipit import Trace


class LocIndexer:
    """Allows for indexing a Trace instance using .loc[]

    Calls pandas.DataFrame.loc for underlying events DataFrame, and wraps result
    in new Trace instance.
    """

    def __init__(self, trace):
        self.trace = trace

    def __getitem__(self, key):
        # Pass argument to events.loc
        item = self.trace.events.loc[key]

        if type(item) == pd.DataFrame:
            # Wrap in new Trace instance
            return Trace(self.trace.definitions, item)

        return item

    def __setitem__(self, key, value):
        self.trace.events.loc[key] = value


class BooleanExpr:
    """Represents a boolean expression that can be evaluated for each event in a
    Trace instance.

    For example:    ("Name", "==", "MPI_Init")
                    ("Process", ">", 5)

    The expression can refer to any field that currently exists in the events
    DataFrame. It supports convenient operations, like comparison and array
    membership.

    BooleanExpr instances can be shared and reused across multiple Traces. Logical
    operators like AND, OR, and NOT can be also be applied to BooleanExpr instances.

    When a Trace is queried with a BooleanExpr, a new Trace instance is created
    containing a view of the events DataFrame of the original Trace. All of Pipit's
    analysis and plotting functions can be applied to the new Trace.
    """

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
            field (str, optional): DataFrame field/column name to operate on.

            operator (str, optional): The operator used to evaluate this expression.
                Allowed operators:
                "<", "<=", "==", ">=", ">", "!=", "in", "not-in", "between"

            value (optional): The value to compare to. For "in" and "not-in"
                operators, this can be a list of any size. For "between", this
                must be a list or tuple of 2 elements, containing the lower and upper
                bound. For all other operators, must be a scalar value, like "MPI_Init"
                or 1.5e+5.

            expr (str, optional): Instead of providing the field, operator, and value,
                you may provide a Pandas query expression, which will be supplied to
                pandas.DataFrame.eval.
                See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.eval.html. # noqa: E501

            validate (optional): Whether or not to validate the Trace. Can be "keep" or False.
        """
        self.field = field
        self.operator = operator
        self.value = value
        self.expr = expr
        self.validate = validate

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __invert__(self):
        return Not(self)

    def __repr__(self):
        if self.expr is not None:
            return f"BooleanExpr {self.expr.__repr__()}"

        else:
            return (
                f"BooleanExpr {self.field.__repr__()} "
                + f"{self.operator} {self.value.__repr__()}"
            )

    def _eval(self, trace):
        """Evaluate this filter on a Trace

        Returns a boolean vector that determines whether each row of the events
        DataFrame should be included in the selection. This result can be supplied
        to `Trace.loc` to get a subset of the Trace.
        """
        value = self.value

        # Parse value into float
        if "time" in self.field.lower():
            value = parse_time(self.value)

        # Get boolean vector using pd.DataFrame.eval
        if self.expr:
            result = trace.events.eval(self.expr)

        # Get boolean vector using pd.Series comparison
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
            field1 = self.field
            field2 = self.field

            if self.field == "Timestamp (ns)":
                trace._match_events()
                field2 = "_matching_timestamp"

            result = (trace.events[field2] >= value[0]) & (
                trace.events[field1] <= value[1]
            )

        else:
            raise Exception("Invalid filter instance")

        # Ensure that if an Enter/Leave row evaluates to True, then so does its
        # matching row
        if self.validate == "keep":
            trace._match_events()
            matching = trace.events[result]["_matching_event"].dropna().tolist()
            result[matching] = True

        return result


class And(BooleanExpr):
    """Combines multiple BooleanExpr objects with a logical AND, such that all of the
    filters must be met."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _eval(self, trace):
        results = [f._eval(trace) for f in self.filters]

        return np.logical_and.reduce(results)

    def __repr__(self):
        return " And ".join(f"({x.__repr__()})" for x in self.filters)


class Or(BooleanExpr):
    """Combines multiple BooleanExpr objects with a logical OR, such that any of the
    filters must be met."""

    def __init__(self, *args):
        super().__init__()
        self.filters = args

    def _eval(self, trace):
        results = [f._eval(trace) for f in self.filters]

        return np.logical_or.reduce(results)

    def __repr__(self):
        return " Or ".join(f"({x.__repr__()})" for x in self.filters)


class Not(BooleanExpr):
    """Inverts BooleanExpr object with a logical NOT, such that the filter must not be
    met."""

    def __init__(self, filter):
        super().__init__()
        self.filter = filter

    def _get_pandas_expr(self, trace):
        return ~self.filter._eval(trace)

    def __repr__(self):
        return f"Not ({self.filter.__repr__()})"
