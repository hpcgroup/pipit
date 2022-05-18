# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pipit.trace


class NsightReader:
    """Reader for NSight Trace Reports (NVTX, CUDA API, GPU)"""

    def __init__(self, nvtx_dir_name, cuda_dir_name, gpu_dir_name):
        """
        https://docs.nvidia.com/nsight-systems/UserGuide/index.html

        The three input files below are CSV files. When a user utilizes
        nsys to profile an application, they can use the -t option to trace
        certain APIs. Once this is done and a qdrep is generated, they can export
        it to a sqlite file if they choose to do so. Finally, they can use nsys stats
        and the nvtxpptrace, cudaapitrace, and gputrace reports to generate the three
        CSV input files for the reader respectively.

        Note: The reader does not currently use thee kernexectrace report as it does not
        seem to provide any new information regarding the trace. All the metrics can be
        calculated using the other trace reports.
        """

        self.nvtx_dir_name = nvtx_dir_name  # directory of nvtx trace report
        self.cuda_dir_name = cuda_dir_name  # directory of cuda trace report
        self.gpu_dir_name = gpu_dir_name  # directory of gpu trace report

    def read(self):
        """
        Generates a Pandas DataFrame containing combined trace information
        from the NVTX, CUDA API, and GPU trace report CSVs that can be
        retrieved using NSight
        """

        # read the csvs into three separate data frames
        nvtx = pd.read_csv(self.nvtx_dir_name, low_memory=False)
        cuda = pd.read_csv(self.cuda_dir_name, low_memory=False)
        gpu = pd.read_csv(self.gpu_dir_name, low_memory=False)

        # perform some preprocessing such as renaming columns
        # and removing unused columns for the nvtx data frame
        nvtx.rename(
            columns={
                "Start:ts_ns": "Start (ns)",
                "End:ts_ns": "End (ns)",
                "PID": "Location Group ID",
                "TID": "Location ID",
            },
            inplace=True,
        )
        nvtx["Location Type"] = "CPU_THREAD"
        nvtx["Location Group Type"] = "PROCESS"
        del nvtx["Duration:dur_ns"]

        # perform some preprocessing such as renaming columns
        # and removing unused columns for the cuda api data frame
        cuda.rename(
            columns={
                "Start Time:ts_ns": "Start (ns)",
                "Pid": "Location Group ID",
                "Tid": "Location ID",
            },
            inplace=True,
        )

        # calculate end time and remove the duration column
        cuda["End (ns)"] = cuda["Start (ns)"] + cuda["Duration:dur_ns"]
        del cuda["Duration:dur_ns"]

        cuda["Location Type"] = "CPU_THREAD"
        cuda["Location Group Type"] = "PROCESS"

        # perform some preprocessing such as renaming columns
        # and removing unused columns for the gpu data frame
        gpu.rename(
            columns={
                "Start:ts_ns": "Start (ns)",
                "Strm": "Location ID",
                "Ctx": "Location Group ID",
            },
            inplace=True,
        )

        # calculate end time and remove the duration column
        gpu["End (ns)"] = gpu["Start (ns)"] + gpu["Duration:dur_ns"]
        del gpu["Duration:dur_ns"]

        gpu["Location Type"] = "ACCELERATOR_STREAM"
        gpu["Location Group Type"] = "ACCELERATOR"

        # set of columns that are common between all three data frames
        commonCols = {
            "Start (ns)",
            "End (ns)",
            "Name",
            "Location ID",
            "Location Type",
            "Location Group ID",
            "Location Group Type",
        }

        """
        create an attributes column that has a dictionary with all
        attributes unique to each data frame (does this by finding which
        columns are not in the common set above)
        """
        nvtxUniqueCols = set(nvtx.columns) - commonCols
        nvtx["Attributes"] = nvtx[nvtxUniqueCols].to_dict(orient="records")

        cudaUniqueCols = set(cuda.columns) - commonCols
        cuda["Attributes"] = cuda[cudaUniqueCols].to_dict(orient="records")

        gpuUniqueCols = set(gpu.columns) - commonCols
        gpu["Attributes"] = gpu[gpuUniqueCols].to_dict(orient="records")

        # merge the reports together to generate the final trace
        self.events = nvtx.filter(
            items=[
                "Start (ns)",
                "End (ns)",
                "Name",
                "Location ID",
                "Location Type",
                "Location Group ID",
                "Location Group Type",
                "Attributes",
            ]
        )
        del nvtx

        self.events = self.events.append(
            cuda.filter(
                items=[
                    "Start (ns)",
                    "End (ns)",
                    "Name",
                    "Location ID",
                    "Location Type",
                    "Location Group ID",
                    "Location Group Type",
                    "Attributes",
                ]
            ),
            ignore_index=True,
        )
        del cuda

        self.events = self.events.append(
            gpu.filter(
                items=[
                    "Start (ns)",
                    "End (ns)",
                    "Name",
                    "Location ID",
                    "Location Type",
                    "Location Group ID",
                    "Location Group Type",
                    "Attributes",
                ]
            ),
            ignore_index=True,
        )
        del gpu

        """
        create a second DataFrame for exit/leave events
        this preserves the structure of a trace and is consistent
        with how the otf2 reader generates the data frame
        """
        leaveDF = pd.DataFrame(
            {
                "Event": np.full((len(self.events),), "Leave"),
                "Timestamp (ns)": self.events["End (ns)"],
                "Name": self.events["Name"],
                "Location ID": self.events["Location ID"],
                "Location Type": self.events["Location Type"],
                "Location Group ID": self.events["Location Group ID"],
                "Location Group Type": self.events["Location Group Type"],
                "Attributes": np.full((len(self.events),), float("NaN")),
            }
        )  # no duplicate attributes to save memory

        # clean up the original data frame with entry events
        self.events.drop(columns=["End (ns)"], inplace=True)
        renameCols = {"Start (ns)": "Timestamp (ns)"}
        self.events.rename(columns=renameCols, inplace=True)

        self.events["Event"] = np.full((len(self.events),), "Enter")
        self.events = self.events[
            [
                "Event",
                "Timestamp (ns)",
                "Name",
                "Location ID",
                "Location Type",
                "Location Group ID",
                "Location Group Type",
                "Attributes",
            ]
        ]  # order of columns in the data frame

        # merge and sort the two data frames (entry and exit events)
        # to generate the final trace
        self.events = self.events.append(leaveDF)
        self.events.sort_values(
            by="Timestamp (ns)", axis=0, ascending=True, inplace=True
        )
        self.events.reset_index(drop=True, inplace=True)

        # conversion to categorical dtype to save memory
        self.events = self.events.astype(
            {
                "Name": "category",
                "Location ID": "category",
                "Location Type": "category",
                "Location Group ID": "category",
                "Location Group Type": "category",
                "Event": "category",
            }
        )

        # return the trace
        return pipit.trace.Trace(None, self.events)
