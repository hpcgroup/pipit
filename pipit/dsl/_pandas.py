from pipit.dsl.dataset import TraceDataset
from pipit.dsl.event import Event
from tabulate import tabulate
import pandas as pd

BUFFER_SIZE = 200


class PandasDataset(TraceDataset):
    def __init__(self, data=pd.DataFrame(), streams=None):
        self.data = data
        self.streams = streams
        self.buffer = []
        self.backend = "pandas"

    def __len__(self) -> int:
        return len(self.data)

    def push_event(self, event: Event) -> None:
        self.buffer.append(event.to_dict())
        if len(self.buffer) >= BUFFER_SIZE:
            self.flush()

    def flush(self) -> None:
        self.data = pd.concat([self.data, pd.DataFrame(self.buffer)])
        self.buffer = []

    def show(self) -> None:
        if len(self.data):
            if len(self.data) > 20:
                top_rows = self.data.head(8).to_dict("records")
                middle_row = {k: "..." for k in self.data.columns}
                bottom_rows = self.data.tail(8).to_dict("records")
                print(
                    tabulate(
                        top_rows + [middle_row] + bottom_rows,
                        headers="keys",
                        tablefmt="psql",
                    )
                )
            else:
                print(
                    tabulate(
                        self.data, headers="keys", tablefmt="psql", showindex=False
                    )
                )

        print(self.__str__())

    def filter(self, condition: str) -> TraceDataset:
        filtered_data = self.data.query(condition)
        return PandasDataset(filtered_data, streams=self.streams)
