import unittest
import datetime
from collections import namedtuple

from cellsite import CellMeasurement, CellIdentity, CellMeasurementSet
from colocation import CellMeasurementPair
from colocation.measurement_pair import MeasurementPairSet
from colocation.triage import (
    select_pair_from_interval_by_right_cell_rarity,
    extract_intervals,
)
from tests.cellsite.util import parse_time


class TriageTest(unittest.TestCase):
    def test_extract_intervals(self):
        ExtractTest = namedtuple(
            "ExtractTest", ["start_timestamp", "duration", "timestamps", "intervals"]
        )
        tests = [
            ExtractTest(
                start_timestamp="2000-01-01 00:00",
                duration=3600,
                timestamps=["2000-01-01 00:01"],
                intervals=[("2000-01-01 00:00", "2000-01-01 01:00")],
            ),
            ExtractTest(
                start_timestamp="2000-01-01 01:00",
                duration=3600,
                timestamps=["2000-01-01 00:01"],
                intervals=[("2000-01-01 00:00", "2000-01-01 01:00")],
            ),
            ExtractTest(
                start_timestamp="2000-01-01 00:00",
                duration=3600,
                timestamps=["2000-01-01 00:00"],
                intervals=[("2000-01-01 00:00", "2000-01-01 01:00")],
            ),
            ExtractTest(
                start_timestamp="2000-01-01 01:00",
                duration=3600,
                timestamps=["2000-01-01 00:00"],
                intervals=[("2000-01-01 00:00", "2000-01-01 01:00")],
            ),
            ExtractTest(
                start_timestamp="1900-01-01 00:00",
                duration=3600,
                timestamps=["2000-01-01 00:00"],
                intervals=[("2000-01-01 00:00", "2000-01-01 01:00")],
            ),
            ExtractTest(
                start_timestamp="2300-01-01 00:00",
                duration=3600,
                timestamps=["2000-01-01 00:00"],
                intervals=[("2000-01-01 00:00", "2000-01-01 01:00")],
            ),
            ExtractTest(
                start_timestamp="2300-01-01 00:00",
                duration=3600,
                timestamps=["2000-01-01 00:00", "2000-01-02 00:00"],
                intervals=[
                    ("2000-01-01 00:00", "2000-01-01 01:00"),
                    ("2000-01-02 00:00", "2000-01-02 01:00"),
                ],
            ),
        ]
        for test in tests:
            expected = [(parse_time(i[0]), parse_time(i[1])) for i in test.intervals]
            returned = extract_intervals(
                timestamps=[parse_time(ts) for ts in test.timestamps],
                start_timestamp=parse_time(test.start_timestamp),
                duration=datetime.timedelta(seconds=test.duration),
            )
            self.assertEqual(expected, returned)

    def test_select_pair_by_cell_rarity(self):
        SelectTest = namedtuple(
            "SelectTest",
            ["start_timestamp", "duration", "measurements", "pairs", "expected"],
        )
        tests = [
            SelectTest(
                start_timestamp="2000-01-01 00:00",
                duration=3600,
                measurements=[
                    ("2000-01-01 00:00", "99-99-99-001"),
                    ("2000-01-01 01:00", "99-99-99-001"),
                ],
                pairs=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-001",
                    )
                ],
                expected=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-001",
                    )
                ],
            ),
            SelectTest(
                start_timestamp="2000-01-01 00:00",
                duration=3600,
                measurements=[
                    ("2000-01-01 00:00", "99-99-99-001"),
                    ("2000-01-01 01:00", "99-99-99-002"),
                ],
                pairs=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 00:00",
                        "99-99-99-002",
                    ),
                    (
                        "2000-01-01 00:01",
                        "99-99-99-002",
                        "2000-01-01 00:01",
                        "99-99-99-001",
                    ),
                    (
                        "2000-01-01 01:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-002",
                    ),
                    (
                        "2000-01-01 01:01",
                        "99-99-99-002",
                        "2000-01-01 01:01",
                        "99-99-99-001",
                    ),
                ],
                expected=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 00:00",
                        "99-99-99-002",
                    ),
                    (
                        "2000-01-01 01:01",
                        "99-99-99-002",
                        "2000-01-01 01:01",
                        "99-99-99-001",
                    ),
                ],
            ),
            SelectTest(
                start_timestamp="2000-01-01 00:00",
                duration=3600,
                measurements=[
                    ("2000-01-01 00:00", "99-99-99-001"),
                    ("2000-01-01 01:00", "99-99-99-002"),
                ],
                pairs=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-002",
                    ),
                    (
                        "2000-01-01 00:01",
                        "99-99-99-002",
                        "2000-01-01 01:01",
                        "99-99-99-001",
                    ),
                    (
                        "2000-01-01 01:00",
                        "99-99-99-001",
                        "2000-01-01 02:00",
                        "99-99-99-002",
                    ),
                    (
                        "2000-01-01 01:01",
                        "99-99-99-002",
                        "2000-01-01 02:01",
                        "99-99-99-001",
                    ),
                ],
                expected=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-002",
                    ),
                    (
                        "2000-01-01 01:01",
                        "99-99-99-002",
                        "2000-01-01 02:01",
                        "99-99-99-001",
                    ),
                ],
            ),
            SelectTest(
                start_timestamp="2000-01-01 00:00",
                duration=3600,
                measurements=[],
                pairs=[
                    (
                        "2000-01-01 00:01",
                        "99-99-99-001",
                        "2000-01-01 01:01",
                        "99-99-99-001",
                    ),
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-001",
                    ),
                    (
                        "2000-01-01 00:01",
                        "99-99-99-001",
                        "2000-01-01 01:01",
                        "99-99-99-001",
                    ),
                ],
                expected=[
                    (
                        "2000-01-01 00:00",
                        "99-99-99-001",
                        "2000-01-01 01:00",
                        "99-99-99-001",
                    ),
                ],
            ),
        ]

        def parse_pairs(raw):
            return [
                CellMeasurementPair(
                    CellMeasurement(parse_time(ts1), CellIdentity.parse(cell1)),
                    CellMeasurement(parse_time(ts2), CellIdentity.parse(cell2)),
                )
                for ts1, cell1, ts2, cell2 in raw
            ]

        for i, test in enumerate(tests):
            measurements = CellMeasurementSet.from_measurements(
                [
                    CellMeasurement(parse_time(ts), CellIdentity.parse(cell))
                    for ts, cell in test.measurements
                ]
            )
            pairs = MeasurementPairSet.from_pairs(parse_pairs(test.pairs))
            intervals = extract_intervals(
                timestamps=[p.left.timestamp for p in pairs],
                start_timestamp=parse_time(test.start_timestamp),
                duration=datetime.timedelta(seconds=test.duration),
            )
            actual = list(
                select_pair_from_interval_by_right_cell_rarity(
                    pairs=pairs,
                    intervals=intervals,
                    background_measurements=measurements,
                )
            )
            expected = parse_pairs(test.expected)

            self.assertEqual(expected, actual, f"test {i}")
