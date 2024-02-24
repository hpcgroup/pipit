from __future__ import annotations
import pandas as pd

# rename to TraceDataset
class Dataset:
    def __init__(self, traces=None) -> None:
        self.traces = traces if traces is not None else dict()

    def __str__(self) -> str:
        return f"Dataset ({len(self)} event{'' if len(self) == 1 else 's'})"

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return sum(len(trace) for trace in self.traces.values())

    @property
    def loc(self):
        pass

    def push_event(self, event) -> None:
        rank = event.rank

        if rank not in self.traces:
            self.traces[rank] = _Trace()

        self.traces[rank].push_event(event)

    def flush(self) -> None:
        for rank in self.traces:
            self.traces[rank].flush()

    def show(self) -> None:
        print(self.__repr__())

    def filter(self, condition) -> Dataset:
        filtered_traces = {
            rank: trace.filter(condition) for rank, trace in self.traces.items()
        }
        return Dataset(filtered_traces)

    def map_ranks(self, f) -> None:
        pass


class _Trace:
    def __init__(self, data=None) -> None:
        self.data = data if data is not None else pd.DataFrame()
        self.buffer = []

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return f"_Trace ({len(self)} event{'' if len(self) == 1 else 's'})"

    def __repr__(self) -> str:
        return str(self)

    @property
    def loc(self):
        pass

    def push_event(self, event) -> None:
        obj = event.to_dict()
        del obj["rank"]

        self.buffer.append(obj)

        if len(self.buffer) >= 200:
            self.flush()

    def flush(self) -> None:
        if len(self.buffer) == 0:
            return

        df = pd.DataFrame(self.buffer)
        df.set_index("idx", inplace=True)
        self.data = pd.concat([self.data, df])
        self.buffer = []

    def show(self) -> None:
        pass

    def filter(self, condition) -> _Trace:
        return _Trace(self.data.query(condition))
