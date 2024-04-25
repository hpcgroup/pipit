import numpy as np


def _match_events_per_rank(args):
    rank, df = args
    matching_events = np.full(len(df), np.nan)

    stack = []
    idx = df.index.tolist()
    event_type = df["Event Type"].tolist()

    for i in range(len(idx)):
        if event_type[i] == "Enter":
            stack.append(idx[i])
        elif event_type[i] == "Leave":
            matching_idx = stack.pop()
            matching_events[idx[i]] = matching_idx
            matching_events[matching_idx] = idx[i]

    df["_matching_event"] = matching_events
    df = df.astype({"_matching_event": "Int32"})

    return rank, df


def _match_caller_callee_per_rank(args):
    rank, df = args

    depth = np.full(len(df), np.nan)
    parent = np.full(len(df), np.nan)

    stack = []
    curr_depth = 0

    idx = df.index.tolist()
    event_type = df["Event Type"].tolist()

    for i in range(len(idx)):
        if event_type[i] == "Enter":
            depth[idx[i]] = curr_depth
            if curr_depth > 0:
                parent[idx[i]] = stack[-1]
            curr_depth += 1
            stack.append(idx[i])
        elif event_type[i] == "Leave":
            stack.pop()
            curr_depth -= 1

    df["_depth"] = depth
    df["_parent"] = parent
    df = df.astype({"_depth": "Int32", "_parent": "Int32"})

    return rank, df
