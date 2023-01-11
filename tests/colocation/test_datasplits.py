import unittest
import datetime
from collections import namedtuple

from cellsite import CellMeasurementSet
from colocation.datasplits import DataWithBackgroundCells
from colocation.measurement_pair import MeasurementPairSet
from colocation.triage import (
    select_pair_from_interval_by_right_cell_rarity,
    extract_intervals,
)
from tests.cellsite.util import parse_measurements, parse_time
from tests.colocation.util import pairs2id, pair2id


class DataWithBackgroundCellsTest(unittest.TestCase):
    def test_with_background(self):
        TestData = namedtuple("TestData", ["measurements", "dislocated_pairs"])
        tests = [
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:03", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-01 02:06", "track1", "device2", 1),
                ],
                dislocated_pairs={
                    (0, 1): [
                        (0, 2),
                    ],
                    (2, 3): [
                        (2, 0),
                    ],
                },
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:59", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-01 02:06", "track1", "device2", 1),
                ],
                dislocated_pairs={
                    (2, 1): [
                        (2, 0),
                    ],
                },
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device1", 1),
                    ("2000-01-01 01:03", "track1", "device2", 1),
                    ("2000-01-01 01:04", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-01 02:06", "track1", "device2", 1),
                    ("2000-01-01 02:07", "track1", "device1", 1),
                    ("2000-01-01 02:08", "track1", "device2", 1),
                ],
                dislocated_pairs={
                    (1, 2): [
                        (1, 4),
                        (1, 6),
                    ],
                    (6, 5): [
                        (6, 0),
                        (6, 1),
                    ],
                },
            ),
        ]
        for test_i, test in enumerate(tests):
            measurements = CellMeasurementSet.from_measurements(
                parse_measurements(test.measurements)
            )
            pairs = MeasurementPairSet.from_sequential_measurements(measurements)
            intervals = extract_intervals(
                timestamps=[m.timestamp for m in measurements],
                start_timestamp=parse_time("2000-01-01 00:00"),
                duration=datetime.timedelta(hours=1),
            )
            selected_pairs = list(
                select_pair_from_interval_by_right_cell_rarity(
                    pairs=pairs.select_by_delay(0, 600),
                    intervals=intervals,
                    background_measurements=measurements,
                )
            )
            self.assertEqual(
                sorted(test.dislocated_pairs.keys()),
                pairs2id(selected_pairs),
                f"test {test_i}: selected pairs from interval by right cell rarity",
            )
            splitter = DataWithBackgroundCells(
                test_pairs=selected_pairs,
                background_measurements=measurements,
                colocated_training_pairs=MeasurementPairSet.from_pairs(
                    [next(iter(pairs))]
                ),
                min_background_delay_secs=600,
            )
            self.assertEqual(
                len(test.dislocated_pairs),
                len(list(splitter)),
                f"test {test_i}: the number of test pairs should match the number of test sets",
            )
            for set_i, (training_pairs, test_pairs) in enumerate(splitter):
                self.assertEqual(
                    1,
                    len(test_pairs),
                    f"test {test_i}.{set_i}: test pairs should be returned one at a time",
                )
                dislocated_training_pairs = pairs2id(
                    training_pairs.select_by_colocation(False)
                )
                self.assertEqual(
                    test.dislocated_pairs[pair2id(test_pairs[0])],
                    dislocated_training_pairs,
                    f"test {test_i}.{set_i}: dislocated training pairs for test pair {pair2id(test_pairs[0])}",
                )
