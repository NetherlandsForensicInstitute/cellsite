import unittest
from itertools import chain
from typing import Iterable, List

from cellsite import CellMeasurementSet, CellMeasurement
from cellsite.measurement_sqlite import SqliteCellMeasurementSet
from tests.cellsite.util import parse_measurements


def measurements2id(measurements: Iterable[CellMeasurement]) -> List[int]:
    return [m.id for m in measurements]


class CellMeasurementTest(unittest.TestCase):
    def test_instantiation(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "sensor1", 1),
        ]
        list0 = parse_measurements(set0)
        set0 = CellMeasurementSet.from_measurements(list0)
        self.assertEqual(list0, list(set0))

    def test_item_operations(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "sensor1", 1),
            ("2000-01-01 01:02", "track1", "sensor1", 1),
            ("2000-01-01 02:01", "track1", "sensor1", 1),
            ("2000-01-02 01:01", "track1", "sensor1", 1),
            ("2000-01-02 02:02", "track1", "sensor1", 1),
            ("2000-01-02 03:02", "track1", "sensor1", 1),
        ]
        set1 = [
            ("2000-01-01 01:01", "track1", "sensor1", 1),
            ("2000-01-01 01:02", "track1", "sensor1", 1),
            ("2000-01-01 02:01", "track1", "sensor1", 1),
            ("2000-01-02 01:01", "track1", "sensor1", 1),
        ]

        measurements0 = CellMeasurementSet.create()
        measurements1 = CellMeasurementSet.from_measurements(parse_measurements(set0))
        measurements2 = CellMeasurementSet.from_measurements(parse_measurements(set1))
        self.assertEqual(0, len(measurements0))
        self.assertEqual(6, len(measurements1))
        self.assertEqual(4, len(measurements2))
        self.assertEqual(6, len(measurements0 + measurements1))
        self.assertEqual(10, len(measurements0 + measurements1 + measurements2))

        measurements0 += measurements1
        measurements0 += measurements2
        self.assertEqual(10, len(measurements0))
        self.assertEqual(6, len(measurements1))
        self.assertEqual(4, len(measurements2))

    def test_set_operations(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "sensor1", 1),
            ("2000-01-01 01:02", "track1", "sensor1", 1),
        ]

        measurements = parse_measurements(set0)
        measurements0 = SqliteCellMeasurementSet([measurements[0]])
        measurements1 = SqliteCellMeasurementSet([measurements[1]])
        default_fields = sorted(measurements0._find_fields().keys())
        measurements0 = measurements0.with_value(attr0="value0")
        self.assertEqual(
            next(iter(measurements0)).attr0,
            "value0",
            "additional attribute is preserved after set modification",
        )
        self.assertEqual(
            sorted(measurements0._find_fields().keys()),
            sorted(chain(default_fields, ["attr0"])),
            "additional attribute is recognized as a field",
        )
        self.assertEqual(
            sorted((measurements0 + measurements1)._find_fields().keys()),
            sorted(chain(default_fields, ["attr0"])),
            "additional attribute is preserved after + operation",
        )
        self.assertEqual(
            sorted((measurements1 + measurements0)._find_fields().keys()),
            sorted(chain(default_fields, ["attr0"])),
            "additional attribute is preserved after reversed + operation",
        )
        self.assertEqual(
            next(iter(measurements0.select_by_track("track1"))).attr0,
            "value0",
            "additional attribute is preserved after set modification",
        )

    def test_select(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "sensor1", 1),
            ("2000-01-01 01:02", "track1", "sensor2", 2),
            ("2000-01-01 02:01", "track1", "sensor3", 3),
            ("2000-01-01 02:01", "track1", "sensor4", 4),
        ]

        set0 = SqliteCellMeasurementSet(parse_measurements(set0))
        track1 = set0.select_by_track("track1")
        self.assertEqual(4, len(set0), "number of measurements")
        self.assertEqual(
            [0, 1, 2, 3],
            measurements2id(set0.sort_by("timestamp")),
            "measurements ordered by timestamp",
        )
        self.assertEqual(
            [3, 2, 1, 0],
            measurements2id(set0.sort_by("timestamp desc")),
            "measurements ordered by timestamp",
        )
        self.assertEqual(4, len(track1), "number of measurements in track1")
        self.assertEqual(
            4,
            len(track1.sort_by("timestamp")),
            "number of sorted measurements in track1",
        )
        self.assertEqual(
            4,
            len(track1.select_by_track("track1")),
            "number of measurements in track1 twice",
        )
        self.assertEqual(
            0, len(set0.select_by_track("track2")), "number of measurements in track2"
        )
        self.assertEqual(
            1,
            len(set0.select_by_sensor("sensor1")),
            "number of measurements in sensor1",
        )
        self.assertEqual(
            1,
            len(track1.select_by_sensor("sensor1")),
            "number of measurements in track1/sensor1",
        )
        self.assertEqual(
            0,
            len(set0.select_by_track("track2").select_by_sensor("sensor1")),
            "number of measurements in track2/sensor1",
        )
        self.assertEqual(
            0,
            len(track1.select_by_sensor("sensor1").select_by_sensor("sensor2")),
            "number of measurements in track2/sensor1/sensor2",
        )
        self.assertEqual(
            2,
            len(track1.select_by_sensor("sensor1", "sensor2")),
            "number of measurements in track1/sensor1 and sensor2",
        )
        self.assertEqual(
            2,
            len(track1.select_by_sensor("sensor1", "sensor2").sort_by("timestamp")),
            "number of sorted measurements in track1/sensor1 and sensor2",
        )

    def test_type_conflict(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "sensor1", 1),
            ("2000-01-01 01:02", "track1", "sensor1", 1),
        ]

        items = parse_measurements(set0)
        items0 = SqliteCellMeasurementSet([items[0]])
        items1 = SqliteCellMeasurementSet([items[1]])
        default_fields = sorted(items0._find_fields().keys())
        items0_with_attr = items0.with_value(attr="value0")
        self.assertEqual(
            "value0",
            next(iter(items0_with_attr)).attr,
            f"additional attribute in `items1` is preserved after set modification",
        )

        items1_with_attr = items1.with_value(attr={"a": "1", "b": 2})
        self.assertEqual(
            {"a": "1", "b": 2},
            next(iter(items1_with_attr)).attr,
            f"additional attribute in `items1` is preserved after set modification",
        )

        self.assertEqual(
            sorted(chain(default_fields, ["attr.a", "attr.b_int", "attr.value"])),
            sorted((items0_with_attr + items1_with_attr)._find_fields().keys()),
        )
