import os
import json
import numpy as np
import pipit.trace
import pandas as pd


class PytorchReader:
    # TODO: parallelization
    def __init__(self, dir_name, create_cct=False):
        self.dir_name = dir_name
        self.files = [file for file in os.listdir(dir_name) if file.endswith(".json")]
        self.create_cct = create_cct

    def read(self):
        dfs = []
        for curr_rank_file in self.files:
            with open(self.dir_name + "/" + curr_rank_file, "r") as file:
                data = json.load(file)
                df = pd.DataFrame(data["traceEvents"])

                complete_events_df = df.loc[df["ph"] == "X"]
                temp_df = complete_events_df[
                    ["ph", "cat", "name", "pid", "tid", "ts"]
                ].copy()

                temp_df["ts"] += complete_events_df["dur"]
                temp_df["ph"].replace({"X": "Leave"}, inplace=True)

                temp_df["args"] = pd.Series(np.full(len(temp_df), np.nan))
                temp_df["id"] = pd.Series(np.full(len(temp_df), np.nan))
                temp_df["bp"] = pd.Series(np.full(len(temp_df), np.nan))
                temp_df["s"] = np.full(len(temp_df), np.nan)

                del df["dur"]
                df["ph"].replace({"X": "Enter", "i": "Instant"}, inplace=True)
                df = pd.concat([df, temp_df], ignore_index=True)

                df["Rank"] = np.full(len(df), data["distributedInfo"]["rank"])

                dfs.append(df)

        df = pd.concat(dfs)

        # or do we want to leave these unchanged
        # since this is technically changing the trace?
        df["ts"] *= 1000
        df["ts"] -= min(df["ts"])
        df.sort_values(by="ts", ignore_index=True, inplace=True)
        df.rename(
            columns={
                "ph": "Event Type",
                "name": "Name",
                "pid": "Process",
                "tid": "Thread",
                "ts": "Timestamp (ns)",
            },
            inplace=True,
        )

        # not very performant right now
        attribute_cols = set(df.columns) - set(
            ["Event Type", "Name", "Rank", "Process", "Thread", "Timestamp (ns)"]
        )
        df["Attributes"] = [
            {k: v for k, v in x.items() if pd.notnull(v)}
            for x in df[list(attribute_cols)].to_dict(orient="records")
        ]
        df.drop(columns=attribute_cols, inplace=True)
        df = df[
            [
                "Timestamp (ns)",
                "Event Type",
                "Name",
                "Rank",
                "Process",
                "Thread",
                "Attributes",
            ]
        ]

        definitions_df = df.loc[df["Event Type"] == "M"]
        definitions_df.reset_index(inplace=True)

        df = df.loc[df["Event Type"] != "M"]
        df.reset_index(inplace=True, drop=True)

        definitions_df.rename(
            columns={"Name": "Definition Type", "args": "Attributes"}, inplace=True
        )
        definitions_df = definitions_df[
            ["Definition Type", "Rank", "Process", "Thread", "Attributes"]
        ]

        trace = pipit.trace.Trace(definitions_df, df)
        if self.create_cct:
            trace.create_cct()

        return trace
