# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pipit.trace

class NSightReader:
    """Reader for NSight trace files"""

    def __init__(self, dir_name) -> None:
        self.dir_name = dir_name
        self.file_name = self.dir_name + "/trace.csv"
        
    """
    This read function directly takes in a csv of the trace report and 
    utilizes pandas to convert it from a csv into a dataframe.
    """
    def read (self):
        file = self.file_name
        df = pd.read_csv(file)
        return pipit.trace.Trace(None, df)
