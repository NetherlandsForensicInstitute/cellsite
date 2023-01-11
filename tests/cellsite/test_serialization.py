import datetime
import os
import unittest

import cellsite
from cellsite import CellMeasurement, CellIdentity, CellMeasurementSet, WgsPoint
from cellsite.serialization import (
    CELL_MEASUREMENT_SERIALIZERS,
    write_measurements_to_csv,
    read_measurements_from_csv,
    deserialize_cell_measurement,
    serialize_cell_measurement,
)
from tests.cellsite.util import parse_measurements, parse_time


class TestSerialization(unittest.TestCase):
    def test_collapse_expand(self):
        now = datetime.datetime.now()
        pairs = [
            (
                {"a": "b"},
                {"a": "b"},
            ),
            (
                {"a": {"1": "a1", "2": "a2"}},
                {"a.1": "a1", "a.2": "a2"},
            ),
            (
                {"a": {"is_available": True, "has_options": False}},
                {"a.is_available": True, "a.has_options": False},
            ),
            (
                {"a": {"some_int_field": 42, "some_float_field": 2.7, "id": 33}},
                {
                    "a.some_int_field_int": 42,
                    "a.some_float_field_float": 2.7,
                    "a.id_int": 33,
                },
            ),
            (
                {"a": {"is_available": None, "some_int_field": None}},
                {"a.is_available": None, "a.some_int_field": None},
            ),
            (
                {"cell": CellIdentity.create("LTE", 204, 1, eci=1)},
                {"cell.identifier": "204-1-1", "cell.radio": "LTE"},
            ),
            (
                CellMeasurement(
                    now,
                    CellIdentity.create("LTE", 204, 1, eci=1),
                    some_field="some_value",
                    some_int_field=42,
                ).as_dict(),
                {
                    "timestamp": now.isoformat(),
                    "cell.radio": "LTE",
                    "cell.identifier": "204-1-1",
                    "some_field": "some_value",
                    "some_int_field_int": 42,
                },
            ),
        ]
        for i, (d_full, d_collapsed) in enumerate(pairs):
            self.assertEqual(
                d_collapsed,
                cellsite.serialization._collapse_dict(
                    d_full, CELL_MEASUREMENT_SERIALIZERS, set()
                ),
                f"test {i}: collapse",
            )
            self.assertEqual(
                d_full,
                cellsite.serialization._expand_dict(
                    d_collapsed, CELL_MEASUREMENT_SERIALIZERS
                ),
                f"test {i}: expand",
            )

    def test_types(self):
        m = CellMeasurement(
            timestamp=parse_time("2000-01-01 01:01"),
            cell=CellIdentity.create(mcc=1, mnc=1),
        )

        self.assertEqual(m, deserialize_cell_measurement(serialize_cell_measurement(m)))
        self.assertEqual(m, next(iter(CellMeasurementSet.from_measurements([m]))))

        m.with_value(wgs84=WgsPoint(lat=52.12345, lon=5.12345))
        self.assertEqual(m, deserialize_cell_measurement(serialize_cell_measurement(m)))
        self.assertEqual(m, next(iter(CellMeasurementSet.from_measurements([m]))))


class TestCSV(unittest.TestCase):
    def test_csv(self):
        measurements = CellMeasurementSet.from_measurements(
            parse_measurements(
                [
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track1", "device2", 1),
                    ("2000-01-02 02:02", "track1", "device1", 1),
                    ("2000-01-02 03:02", "track1", "device2", 1),
                ]
            )
        )
        try:
            write_measurements_to_csv("temporary_file.csv", measurements)
            self.assertEqual(
                list(measurements.with_value(id=None)),
                list(
                    read_measurements_from_csv("temporary_file.csv").with_value(id=None)
                ),
            )
        finally:
            if os.path.exists("temporary_file.csv"):
                os.remove("temporary_file.csv")
