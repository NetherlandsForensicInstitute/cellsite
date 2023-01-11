import datetime
from collections import defaultdict
from functools import partial
from typing import List, Tuple, Sequence, Iterable, Callable, Optional, Set

from tqdm import tqdm

from cellsite import CellMeasurement, CellMeasurementSet, CellIdentity
from colocation import MeasurementPairSet


def extract_intervals(
    timestamps: List[datetime.datetime],
    start_timestamp: datetime.datetime,
    duration: datetime.timedelta,
) -> List[Tuple[datetime.datetime, datetime.datetime]]:
    """
    Calculates, for a set of intervals and timestamps, which intervals have at least one timestamp.

    The first interval is defined by a `start_timestamp` and a duration. The next interval starts adjacent to (before or
    after) the first interval, and so on. An interval is returned iff there is at least one timestamp in `timestamps`
    that is within the interval.

    @param timestamps: a list of timestamps which determine which intervals are returned
    @param start_timestamp: the start of the first interval
    @param duration: the duration of the intervals
    @return: a list of intervals for which there is at least one timestamp
    """
    intervals = set()
    for ts in timestamps:
        sequence_no = (ts - start_timestamp) // duration
        interval_start = start_timestamp + sequence_no * duration
        intervals.add((interval_start, interval_start + duration))

    return sorted(intervals)


def get_cell_frequencies(measurements: CellMeasurementSet) -> dict[CellIdentity, int]:
    all_cells = measurements.get_cells()
    counts_by_cell = {}
    for cell in all_cells:
        counts_by_cell[cell] = len(measurements.select_by_cell(cell))
    return counts_by_cell


def select_pair_from_interval_by_right_cell_rarity(
    pairs: MeasurementPairSet,
    background_measurements: CellMeasurementSet,
    intervals: Sequence[Tuple[datetime.datetime, datetime.datetime]],
):
    """
    Select items from a set of measurement `pairs`, such that for each interval:
    - exactly one pair is selected if a pair exists for which the *left* measurement is within the interval,
    - otherwise no pair is selected,
    - there is no pair for the same interval for which the cell in the *right* measurement is more frequent in `background_measurements` *outside* the interval
    - if there is no single pair with the lowest right cell frequency, the pair with the smallest delay is selected
    - if there is no single pair with the lowest right cell frequency and the lowest delay, the first pair (chronologically by left measurement) is selected

    @param pairs: the list of pairs to be filtered
    @param intervals: the intervals to group pairs by
    @param background_measurements: the measurements to use for cell frequency counts
    @return: at most one pair for each interval
    """
    cell_frequencies = get_cell_frequencies(background_measurements)
    for interval in intervals:
        cell_frequencies_for_interval = dict(cell_frequencies)
        for measurement in background_measurements.select_by_timestamp(*interval):
            cell_frequencies_for_interval[measurement.cell] -= 1

        candidates = pairs.select_by_left_timestamp(*interval)
        candidates = [
            (pair, cell_frequencies_for_interval.get(pair.right.cell, 0))
            for pair in candidates
        ]
        candidates = sorted(
            candidates, key=lambda x: (-x[1], x[0].delay, x[0].left.timestamp)
        )
        if len(candidates) > 0:
            yield candidates[0][0]


def select_pair_from_interval_and_devices_by_right_cell_rarity(
    pairs: MeasurementPairSet,
    background_measurements: CellMeasurementSet,
    intervals: Sequence[Tuple[datetime.datetime, datetime.datetime]],
    progress_bar: Callable = lambda x: x,
):
    """
    Same ase `select_pair_from_interval_by_right_cell_rarity`, except that it selects a pair for each combination
    of two devices.

    @param pairs: the list of pairs to be filtered
    @param intervals: the intervals to group pairs by
    @param background_measurements: the measurements to use for cell frequency counts
    @param progress_bar: show progress bar
    @return: at most one pair for each interval
    """
    for right_device in progress_bar(pairs.device_names):
        background_measurements_for_device = background_measurements.select_by_device(
            right_device
        )
        # sub select by right device
        pairs_for_right_device = pairs.select_by_right_device(right_device)

        # if there are no pairs, don´t bother
        if len(pairs_for_right_device) == 0:
            continue

        cell_frequencies = get_cell_frequencies(background_measurements_for_device)

        for left_device in pairs.device_names:
            # sub select by left device
            pairs_for_devices = pairs_for_right_device.select_by_left_device(
                left_device
            )

            # if there are no pairs, don´t bother
            if len(pairs_for_devices) == 0:
                continue

            for interval in intervals:
                # all pairs for the devices within the interval are candidates
                candidates = pairs_for_devices.select_by_left_timestamp(*interval)

                # reduce the cell frequencies by the counts in the current interval, because they should be excluded
                cell_frequencies_for_interval = dict(cell_frequencies)
                for (
                    measurement
                ) in background_measurements_for_device.select_by_timestamp(*interval):
                    cell_frequencies_for_interval[measurement.cell] -= 1

                # assign the right cell frequency to every pair
                candidates = [
                    (pair, cell_frequencies_for_interval[pair.right.cell])
                    for pair in candidates
                ]

                # order by frequency
                candidates = sorted(
                    candidates, key=lambda x: (-x[1], x[0].delay, x[0].left.timestamp)
                )

                # select the rarest, if any
                if len(candidates) > 0:
                    yield candidates[0][0]
