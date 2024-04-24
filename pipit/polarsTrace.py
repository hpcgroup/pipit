import polars as pl
import pandas as pd
import numpy as np


def pd_to_polars(_df):
    """
    Convert a Pandas DataFrame to Polars DataFrame and handle columns
    with int and float categorical dtypes.
    """
    _df = _df.copy()
    for col in _df.columns:
        if isinstance(_df[col].dtype, pd.CategoricalDtype):
            if pd.api.types.is_integer_dtype(_df[col].cat.categories.dtype):
                _df[col] = _df[col].astype(int)
                print(f"Column [{col}] cast to int")
            elif pd.api.types.is_float_dtype(_df[col].cat.categories.dtype):
                _df[col] = _df[col].astype(float)
                print(f"Column [{col}] cast to float")

    return pl.from_pandas(_df)


class PolarsTrace:
    def __init__(self, df: pd.DataFrame):
        self.events = pd_to_polars(df).with_row_index("index")

    def _match_events_naive(self):
        matching_events = np.full(self.events.height, -1, dtype=np.int32)

        processes = self.events.select("Process").unique().to_series(0).to_list()
        enterLeave = self.events.filter(pl.col("Event Type").is_in(["Enter", "Leave"]))

        for process in processes:
            tmp = enterLeave.filter(pl.col("Process") == process).select(
                ["index", "Event Type"]
            )
            stack = []

            for idx, event_type in tmp.iter_rows(named=False):
                if event_type == "Enter":
                    stack.append(idx)
                elif event_type == "Leave":
                    matching_idx = stack.pop()
                    matching_events[idx] = matching_idx
                    matching_events[matching_idx] = idx

        self.events = self.events.with_columns(
            pl.Series(name="_matching_event", values=matching_events)
            .replace(-1, pl.lit(None))
            .cast(pl.UInt32)
        ).with_columns(pl.col("_matching_event").cast(pl.UInt32))

    def _match_events(self):
        matching_events = np.full(self.events.height, -1, dtype=np.int32)

        def helper(df):
            tmp = df.select(["index", "Event Type"])
            stack = []

            for idx, event_type in tmp.iter_rows(named=False):
                if event_type == "Enter":
                    stack.append(idx)
                elif event_type == "Leave":
                    matching_idx = stack.pop()
                    matching_events[idx] = matching_idx
                    matching_events[matching_idx] = idx

            return df

        self.events.filter(pl.col("Event Type").is_in(["Enter", "Leave"])).group_by(
            "Process"
        ).map_groups(helper)

        self.events = self.events.with_columns(
            pl.Series(name="_matching_event", values=matching_events)
            .replace(-1, pl.lit(None))
            .cast(pl.UInt32)
        ).with_columns(pl.col("_matching_event").cast(pl.UInt32))

    def _match_caller_callee(self):
        depth = np.full(self.events.height, -1, dtype=np.int32)
        parent = np.full(self.events.height, -1, dtype=np.int32)

        def match_caller_callee(df):
            tmp = df.select(["index", "Event Type"])
            curr_depth = 0
            stack = []

            for idx, event_type in tmp.iter_rows(named=False):
                if event_type == "Enter":
                    depth[idx] = curr_depth
                    if curr_depth > 0:
                        parent[idx] = stack[-1]
                    curr_depth += 1
                    stack.append(idx)
                elif event_type == "Leave":
                    stack.pop()
                    curr_depth -= 1

            return df

        self.events.filter(pl.col("Event Type").is_in(["Enter", "Leave"])).group_by(
            "Process"
        ).map_groups(match_caller_callee)

        self.events = self.events.with_columns(
            [
                pl.Series(name="_depth", values=depth)
                .replace(-1, pl.lit(None))
                .cast(pl.UInt32),
                pl.Series(name="_parent", values=parent)
                .replace(-1, pl.lit(None))
                .cast(pl.UInt32),
            ]
        ).with_columns(pl.col("_matching_event").cast(pl.UInt32))

    def calc_inc_metrics(self):
        cols_to_keep = {
            "Timestamp (ns)_right": "_matching_timestamp",
        }

        self.events = (
            self.events.join(
                self.events.filter(pl.col("Event Type") == "Leave"),
                left_on="index",
                right_on="_matching_event",
                how="left",
            )
            .rename(cols_to_keep)
            .drop(pl.selectors.contains("_right"))
            .with_columns(
                (pl.col("_matching_timestamp") - pl.col("Timestamp (ns)")).alias("time.inc")
            )
        )

    def calc_exc_metrics(self):
        children_times = self.events.group_by("_parent").agg(pl.col("Timestamp (ns)").sum())
        self.events = self.events.join(
            children_times,
            left_on="index",
            right_on="_parent",
            how="left"
        ).rename({
            "Timestamp (ns)_right": "children_time"
        }).with_columns(
            (pl.col("time.inc") - pl.col("children_time"))
            .fill_null(pl.col("time.inc"))
            .alias("time.exc")
        ).drop("children_time")