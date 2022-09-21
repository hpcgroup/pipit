# Copyright 2022 Parallel Software and Systems Group, University of Maryland.
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# Used to create fake traces to test nsight-reader

import random as rand
import csv


class FakeCreator:
    def __init__(self, func_names):
        self.func_names = func_names

    def create_trace(self):
        # Creating the data for the csv. Column Names
        data = [["Name", "Start (ns)", "End (ns)", "RangeStack"]]

        # 10,000 value is placeholder for end time
        data.append(["main", 0, 10000, ":1"])

        # Grabbing 8 function names from list
        funcs = rand.sample(self.func_names, 8)

        # setting random function time for the first inner function
        dur = rand.randint(1000, 2000)

        data.append([funcs[0], 5, dur + 5, ":1:2"])

        # Used to store data of inner function calls
        func_data = []

        # used to calculate skips over inner function calls
        set_back = 0

        # Creating Function with times
        for i in range(1, len(funcs)):
            # Create a list to store inner function calls
            func = [funcs[i]]

            # Random Number to determine parent function has children func or not
            func.append(rand.randint(0, 1))

            # Function has inner function calls
            if func[1]:
                # Getting Inner Function Names
                inner_fun = rand.sample(self.func_names, rand.randint(1, 3))

                # Make sure there are no calls of parent function as a child
                while funcs[i] in inner_fun:
                    inner_fun = rand.sample(self.func_names, rand.randint(1, 3))

                # Check if function is called within child

                # Getting Inner Function Duration Time
                dur = rand.sample(range(500, 800), len(inner_fun))

                # Getting Full Time of Functions
                full_dur = sum(dur)

                # Getting full time of parent Function with padding
                full_dur_rand = rand.randint(full_dur + 100, full_dur + 500)

                # Appending data to func to store in func_data list
                # Store of where parent function is in list
                func.append(i + 2)
                func.append(full_dur_rand)
                func.append(inner_fun)
                func.append(dur)

                func_data.append(func)

                # Update Range Stack for new function in data
                range_stack = data[i + 1][3]
                range_new = str(int(range_stack.split(":")[-1]) + 1 + set_back)

                # Appending Function Name, Start Time, End Time, Range Stack
                data.append(
                    [
                        funcs[i],
                        data[i + 1][2] + 10,
                        data[i + 1][2] + full_dur_rand,
                        range_stack[: -(len(range_stack.split(":")[-1]))] + range_new,
                    ]
                )

                # Updating set back
                set_back = len(inner_fun)

            # Function does not have inner function calls
            else:
                # Creating Duration Time
                dur = rand.randint(1000, 3000)

                # Updating Range Stack
                range_stack = data[i + 1][3]
                range_new = str(int(range_stack.split(":")[-1]) + 1 + set_back)

                # Resetting set back
                set_back = 0

                # Appending Function Name, Start Time, End Time, Range Stack
                data.append(
                    [
                        funcs[i],
                        data[i + 1][2] + 10,
                        data[i + 1][2] + dur,
                        range_stack[: -(len(range_stack.split(":")[-1]))] + range_new,
                    ]
                )

        # Updating Main Function end time
        data[1][2] = data[-1][2] + 300

        func_data.reverse()

        # Adding Children to Data
        for i in func_data:
            # Getting Children Function Names
            fun = i[4]
            # Getting Children Duration Time
            dur = i[5]
            # Getting Start Time of Parent Function
            time = data[i[2]][1]
            # Getting Index of where Parent Function is in Data List
            ind = i[2] + 1

            # Getting Creating Range Stack for Children
            r_stack = data[i[2]][3]
            last = r_stack.split(":")[-1]
            r_stack = r_stack + ":" + last

            # Adding Each child to the list one by one
            for j in range(len(fun)):
                # Creating Start and End Times for each function
                time += 10
                start = time
                time += dur[j]
                time += 10
                end = time

                # Creating new Range Stack
                r_new = str(int(r_stack.split(":")[-1]) + 1)

                r_stack = r_stack[: -(len(r_stack.split(":")[-1]))] + r_new

                # Inserting Data
                data.insert(ind, [fun[j], start, end, r_stack])

                # Updating Index to insert data
                ind += 1

        with open(
            "../../pipit/tests/data/nbody-nvtx/fake_trace.csv",
            "w",
            encoding="UTF8",
            newline="",
        ) as f:
            writer = csv.writer(f)
            writer.writerows(data)