from pipit import Trace
import numpy as np
from faketest import gen_fake_tree, emit_tree_file
import pandas as pd


def test_with_fake_data():
    """
    Generate a fake test file and ground truth file, read the test file
    with Pipit, and check it against the ground truth. Tests inclusive and
    exclusive metrics, and uses time_profile_test_generic.
    """
    num_processes = 8
    # generate one fake tree per process, 2000 functions in the tree
    trees = [gen_fake_tree(2000) for n in range(num_processes)]
    test_file = open("fake.csv", "w")
    ground_truth = open("fake_ground.csv", "w")
    emit_tree_file(trees, test_file, ground_truth)
    test_file.close()
    ground_truth.close()
    trace = Trace.from_csv("fake.csv")
    # gt_dataframe should hold identical values to the columns of trace.events
    gt_dataframe = pd.read_csv("fake_ground.csv")
    trace.calc_exc_metrics()
    pipit_dataframe = trace.events[["time.inc", "time.exc"]]
    # adjust for nanoseconds
    gt_dataframe["time.inc"] *= 1e9
    gt_dataframe["time.exc"] *= 1e9
    # NaN values for time won't compare equal, so check ourselves
    assert (
        np.isclose(pipit_dataframe["time.inc"], gt_dataframe["time.inc"])
        | (np.isnan(gt_dataframe["time.inc"]) & np.isnan(pipit_dataframe["time.inc"]))
    ).all()
    # likewise, check exclusive metrics
    assert (
        np.isclose(pipit_dataframe["time.exc"], gt_dataframe["time.exc"])
        | (np.isnan(gt_dataframe["time.exc"]) & np.isnan(pipit_dataframe["time.exc"]))
    ).all()
    time_profile_test_generic(trace, num_processes)


def time_profile_test_generic(trace, num_processes):
    """
    Tests universal properties of time_profile, regardless of the trace.
    Most asserts were taken from pipit/tests/trace.py, except those specific
    to the ping-pong trace.
    """
    trace.calc_exc_metrics(["Timestamp (ns)"])

    time_profile = trace.time_profile(num_bins=62)

    # check length
    assert len(time_profile) == 62

    # check bin sizes
    exp_duration = (
        trace.events["Timestamp (ns)"].max() - trace.events["Timestamp (ns)"].min()
    )
    exp_bin_size = exp_duration / 62
    bin_sizes = time_profile["bin_end"] - time_profile["bin_start"]

    assert np.isclose(bin_sizes, exp_bin_size).all()

    # check that sum of function contributions per bin equals bin duration
    exp_bin_total_duration = exp_bin_size * num_processes
    time_profile.drop(columns=["bin_start", "bin_end"], inplace=True)

    assert np.isclose(time_profile.sum(axis=1), exp_bin_total_duration).all()

    # check for each function that sum of exc time per bin equals total exc time
    total_exc_times = trace.events.groupby("Name")["time.exc"].sum()

    for column in time_profile:
        if column == "idle_time":
            continue

        assert np.isclose(time_profile[column].sum(), total_exc_times[column])

    # check normalization
    norm = trace.time_profile(num_bins=62, normalized=True)
    norm.drop(columns=["bin_start", "bin_end"], inplace=True)

    assert (time_profile / exp_bin_total_duration).equals(norm)
