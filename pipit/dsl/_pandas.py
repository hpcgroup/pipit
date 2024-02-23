from pipit.dsl.dataset import TraceData
from pipit.dsl.event import Event
from pipit.dsl.expr import Expr
from tabulate import tabulate
import pandas as pd

BUFFER_SIZE = 200

class PandasDataset(TraceData):
    def __init__(self, data=pd.DataFrame()):
        self.data = data
        self.buffer = []
        
    def push_event(self, event: Event) -> None:
        self.buffer.append(event.to_dict())
        if len(self.buffer) >= BUFFER_SIZE:
            self.flush()

    def flush(self) -> None:
        self.data = pd.concat([self.data, pd.DataFrame(self.buffer)])
        self.buffer = []

    def show(self) -> None:
        if len(self.data) == 0:
            print("(Empty dataset)")

        print(tabulate(self.data, headers='keys', tablefmt='psql'))
    
    def filter(self, condition: str) -> TraceData:
        filtered_data = self.data.query(condition)
        return PandasDataset(filtered_data)