import os
import json
import numpy as np
import pipit.trace
import pandas as pd
import multiprocessing as mp


# TODO: comments
# TODO: unit tests - compare with HTA
class PytorchReader:
    def __init__(self, dir_name, num_processes=None, create_cct=False):
        self.dir_name = dir_name
        self.files = [file for file in os.listdir(dir_name) if file.endswith(".json")]
        self.create_cct = create_cct

        num_cpus = mp.cpu_count()
        if num_processes is None or num_processes < 1 or num_processes > num_cpus:
            self.num_processes = num_cpus
        else:
            self.num_processes = num_processes

        if self.num_processes > len(self.files):
            self.num_processes = len(self.files)

    def events_reader(self, rank_size):
        # TODO: this pattern is in a lot of readers now - consolidate into one function
        rank, size = rank_size[0], rank_size[1]
        per_process = int(len(self.files) // size)
        remainder = int(len(self.files) % size)

        if rank < remainder:
            begin_int = rank * (per_process + 1)
            end_int = (rank + 1) * (per_process + 1)
        else:
            begin_int = (rank * per_process) + remainder
            end_int = ((rank + 1) * per_process) + remainder

        dfs = []
        for curr_rank_file in self.files[begin_int:end_int]:
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
                df["ph"].replace(
                    {"X": "Enter", "i": "Instant", "B": "Enter", "E": "Leave"},
                    inplace=True,
                )

                complete_events_indices = complete_events_df.index.values

                new_df_index = np.full(len(df.index.values), 0)
                new_df_index[complete_events_indices] = 1
                new_df_index = np.roll(new_df_index.cumsum(), 1)
                new_df_index[0] = 0

                df.index = df.index.values + new_df_index

                temp_df.index = df.index.values[complete_events_indices] + 1

                df = pd.concat([df, temp_df])
                df.sort_index(inplace=True)

                df["Rank"] = np.full(len(df), data["distributedInfo"]["rank"])

                dfs.append(df)

        df = pd.concat(dfs)

        df["ts"] *= 1000

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

        # TODO: is there a more performant way of doing this?
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

        return df

    def read(self):
        pool_size, pool = self.num_processes, mp.Pool(self.num_processes)

        events_dfs = pool.map(
            self.events_reader, [(rank, pool_size) for rank in range(pool_size)]
        )

        pool.close()

        events_df = pd.concat(events_dfs)
        del events_dfs

        # stable sorting so that order of events with same timestamps isn't "corrupted"
        # TODO: this change needs to be made in all readers
        events_df.sort_values(
            by="Timestamp (ns)", ignore_index=True, inplace=True, kind="stable"
        )

        definitions_df = events_df.loc[events_df["Event Type"] == "M"]
        definitions_df.reset_index(inplace=True)

        events_df = events_df.loc[events_df["Event Type"] != "M"]
        events_df.reset_index(inplace=True, drop=True)

        events_df = events_df.astype(
            {
                "Event Type": "category",
                "Name": "category",
                "Rank": "category",
                "Process": "category",
                "Thread": "category",
            }
        )

        definitions_df.rename(
            columns={"Name": "Definition Type", "args": "Attributes"}, inplace=True
        )
        definitions_df = definitions_df[
            ["Definition Type", "Rank", "Process", "Thread", "Attributes"]
        ]

        definitions_df = definitions_df.astype(
            {
                "Definition Type": "category",
                "Rank": "category",
                "Process": "category",
                "Thread": "category",
            }
        )

        trace = pipit.trace.Trace(definitions_df, events_df)
        if self.create_cct:
            trace.create_cct()

        return trace
