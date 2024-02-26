# Implement trace analysis functions from trace.py in this file
# Just for helping develop the DSL API, not for production use
# eventually all of this functionality will replace the trace.py file
from typing import List, Tuple
from pipit.dsl2.dataset import TraceDataset
from pipit.dsl2.reduce import DictLike


# Philosophy: High-level class for trace analysis, built on top of the
# low-level DSL. Should be agnostic to the underlying backend used
# in the DSL.
# May not use Pandas or Numpy directly, but may use them indirectly
# through the DSL.
class Analyzer:
    def __init__(self, ds: TraceDataset) -> None:
        self.ds = ds

    def _match_events(self) -> None:
        """
        Match enter and leave events in all traces.
        """

        def _match_events_per_trace(trace):
            matching_evt = [-1] * trace.count()
            matching_ts = [float("nan")] * trace.count()

            stack = []

            for event in trace.iter_events():
                if event.event_type == "Enter":
                    stack.append((event.idx, event.timestamp))
                elif event.event_type == "Leave":
                    enter_idx, enter_ts = stack.pop()
                    matching_evt[enter_idx] = event.idx
                    matching_ts[enter_idx] = event.timestamp
                    matching_evt[event.idx] = enter_idx
                    matching_ts[event.idx] = enter_ts

            trace.add_column("matching_evt", matching_evt)
            trace.add_column("matching_ts", matching_ts)

            del matching_evt
            del matching_ts

        self.ds.map_traces(_match_events_per_trace)

    def _match_caller_callee(self) -> None:
        """
        Match caller and callee events in all traces.
        """

        def _match_caller_callee_per_trace(trace):
            depth = [0] * trace.count()
            par = [-1] * trace.count()
            children = [[] for _ in range(trace.count())]

            stack = []

            for event in trace.iter_events():
                if event.event_type == "Enter":
                    if stack:
                        par[event.idx] = stack[-1][0]
                        children[stack[-1][0]].append(event.idx)
                    depth[event.idx] = len(stack)

                    stack.append((event.idx, event.timestamp))
                elif event.event_type == "Leave":
                    stack.pop()

            trace.add_column("depth", depth)
            trace.add_column("par", par)
            trace.add_column("children", children)

            del depth
            del par
            del children

        self.ds.map_traces(_match_caller_callee_per_trace)

    def calc_inc_time(self) -> None:
        pass

    def calc_exc_time(self) -> None:
        pass

    def comm_matrix(self) -> List[List[int]]:
        pass

    def message_histogram(self) -> Tuple[List[float], List[float]]:
        pass

    def comm_over_time(self) -> Tuple[List[float], List[float]]:
        pass

    def comm_by_rank(self) -> DictLike:
        pass

    def flat_profile(self) -> DictLike:
        pass

    def load_imbalance(self) -> DictLike:
        pass

    def idle_time(self) -> DictLike:
        pass

    def time_profile(self) -> DictLike:
        pass
