import unittest
from collections import namedtuple
from itertools import chain

from cellsite import CellMeasurementSet
from cellsite.serialization import _collapse_dict, CELL_MEASUREMENT_SERIALIZERS
from colocation.measurement_pair import (
    MeasurementPairSet,
    CellMeasurementPair,
)
from colocation.serialization import PAIR_SERIALIZERS
from tests.cellsite.util import parse_measurements
from tests.colocation.util import pairs2id


def keys_for_set(items):
    item = next(iter(items))
    dict_item = item.as_dict()
    collapsed = _collapse_dict(dict_item, PAIR_SERIALIZERS, set())
    return sorted(collapsed.keys())


class PairingTest(unittest.TestCase):
    def test_pair_set_from_sequential_measurements(self):
        TestData = namedtuple("CombineTest", ["measurements", "pairs"])
        tests = [
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device1", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track1", "device1", 1),
                    ("2000-01-02 02:02", "track1", "device1", 1),
                    ("2000-01-02 03:02", "track1", "device1", 1),
                ],
                pairs=[],
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track1", "device2", 1),
                    ("2000-01-02 02:02", "track1", "device1", 1),
                    ("2000-01-02 03:02", "track1", "device2", 1),
                ],
                pairs=[
                    (0, 1),
                    (2, 1),
                    (2, 3),
                    (4, 3),
                    (4, 5),
                ],
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track2", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track2", "device2", 1),
                    ("2000-01-02 02:02", "track1", "device1", 1),
                    ("2000-01-02 03:02", "track2", "device2", 1),
                ],
                pairs=[],
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device3", 1),
                    ("2000-01-01 02:01", "track1", "device4", 1),
                ],
                pairs=[
                    (0, 1),
                    (0, 2),
                    (0, 3),
                    (1, 2),
                    (1, 3),
                    (2, 3),
                ],
            ),
        ]
        for i, test in enumerate(tests):
            if i < 2:
                continue
            measurements = CellMeasurementSet.from_measurements(
                parse_measurements(test.measurements)
            )
            pairs = MeasurementPairSet.from_sequential_measurements(measurements)
            pairs = sorted(pairs2id(pairs))
            self.assertEqual(test.pairs, pairs, f"test {i}; expected {test.pairs}")

    def test_pair_set_from_sequential_measurements_across_tracks(self):
        TestData = namedtuple("CombineTest", ["measurements", "pairs"])
        tests = [
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device1", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track2", "device1", 1),
                    ("2000-01-02 02:02", "track2", "device1", 1),
                    ("2000-01-02 03:02", "track2", "device1", 1),
                ],
                pairs=[],
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track2", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track2", "device2", 1),
                    ("2000-01-02 02:02", "track1", "device1", 1),
                    ("2000-01-02 03:02", "track2", "device2", 1),
                ],
                pairs=[
                    (0, 1, False),
                    (2, 1, False),
                    (2, 3, False),
                    (4, 3, False),
                    (4, 5, False),
                ],
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track1", "device2", 1),
                    ("2000-01-01 02:01", "track1", "device1", 1),
                    ("2000-01-02 01:01", "track2", "device2", 1),
                    ("2000-01-02 02:02", "track1", "device1", 1),
                    ("2000-01-02 03:02", "track2", "device2", 1),
                ],
                pairs=[
                    (0, 1, True),
                    (2, 1, True),
                    (2, 3, False),
                    (4, 3, False),
                    (4, 5, False),
                ],
            ),
            TestData(
                measurements=[
                    ("2000-01-01 01:01", "track1", "device1", 1),
                    ("2000-01-01 01:02", "track2", "device2", 1),
                    ("2000-01-01 02:01", "track3", "device3", 1),
                    ("2000-01-01 02:01", "track4", "device4", 1),
                ],
                pairs=[
                    (0, 1, False),
                    (0, 2, False),
                    (0, 3, False),
                    (1, 2, False),
                    (1, 3, False),
                    (2, 3, False),
                ],
            ),
        ]
        for i, test in enumerate(tests):
            if i < 2:
                continue
            measurements = CellMeasurementSet.from_measurements(
                parse_measurements(test.measurements)
            )
            pairs = MeasurementPairSet.from_sequential_measurements(
                measurements, within_track=False
            )
            pairs = sorted(
                (pair.left.id, pair.right.id, pair.is_colocated) for pair in pairs
            )
            self.assertEqual(test.pairs, pairs, f"test {i}; expected {test.pairs}")

    def test_select_by_delay(self):
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

        TestData = namedtuple("CombineTest", ["delay", "pairs"])
        tests = [
            TestData(
                delay=(None, None),
                pairs=[
                    (0, 1),
                    (2, 1),
                    (2, 3),
                    (4, 3),
                    (4, 5),
                ],
            ),
            TestData(
                delay=(0, 3600),
                pairs=[
                    (0, 1),
                    (2, 1),
                ],
            ),
            TestData(
                delay=(0, 600),
                pairs=[
                    (0, 1),
                ],
            ),
        ]
        for test in tests:
            pairs = MeasurementPairSet.from_sequential_measurements(measurements)
            pairs = pairs.select_by_delay(*test.delay)
            pairs = sorted(pairs2id(pairs))
            self.assertEqual(test.pairs, pairs)

    def test_set_operations(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "device1", 1),
            ("2000-01-01 01:02", "track1", "device1", 1),
        ]

        items = parse_measurements(set0)
        items0 = MeasurementPairSet.from_pairs(
            [CellMeasurementPair(left=items[0], right=items[0], is_colocated=True)]
        )
        items1 = MeasurementPairSet.from_pairs(
            [CellMeasurementPair(left=items[1], right=items[1], is_colocated=True)]
        )
        default_fields = keys_for_set(items0)
        for attr_name, attr_value in [
            ("string_attribute", "value0"),
            ("dict_attribute", {"a": "1", "b": "2"}),
        ]:
            additional_fields = (
                sorted(
                    f"{attr_name}.{key}"
                    for key in _collapse_dict(
                        attr_value, CELL_MEASUREMENT_SERIALIZERS, set()
                    ).keys()
                )
                if isinstance(attr_value, dict)
                else [attr_name]
            )
            items0_with_attr = items0.with_value(**{attr_name: attr_value})
            self.assertEqual(
                getattr(next(iter(items0_with_attr)), attr_name),
                attr_value,
                f"test `with_value`: additional attribute {attr_name} is preserved after set modification",
            )
            self.assertEqual(
                keys_for_set(items0_with_attr),
                sorted(chain(default_fields, additional_fields)),
                f"test `with_value`: additional attribute {attr_name} is recognized as a field",
            )
            self.assertEqual(
                keys_for_set(items0_with_attr + items1),
                sorted(chain(default_fields, additional_fields)),
                f"test `with_value`: additional attribute {attr_name} is preserved after + operation",
            )
            self.assertEqual(
                keys_for_set(items1 + items0_with_attr),
                sorted(chain(default_fields, additional_fields)),
                f"test `with_value`: additional attribute {attr_name} is preserved after reversed + operation",
            )
            self.assertEqual(
                getattr(
                    next(iter(items0_with_attr.select_by_colocation(True))), attr_name
                ),
                attr_value,
                f"test `with_value`: additional attribute {attr_name} is preserved after set modification",
            )
            self.assertEqual(
                getattr(
                    next(iter(items0_with_attr.select_by_delay(0, 3600))), attr_name
                ),
                attr_value,
                f"test `with_value`: additional attribute {attr_name} is preserved after set modification",
            )

            additional_fields = (
                sorted(
                    f"left_msrmnt.{attr_name}.{key}"
                    for key in _collapse_dict(
                        attr_value, CELL_MEASUREMENT_SERIALIZERS, set()
                    ).keys()
                )
                if isinstance(attr_value, dict)
                else [f"left_msrmnt.{attr_name}"]
            )
            items0_with_attr = items0.apply(
                lambda pair: pair.with_value(
                    left=pair.left.with_value(**{attr_name: attr_value})
                )
            )
            self.assertEqual(
                attr_value,
                getattr(next(iter(items0_with_attr)).left, attr_name),
                f"test `apply`: additional attribute {attr_name} is preserved after set modification",
            )
            self.assertEqual(
                sorted(chain(default_fields, additional_fields)),
                keys_for_set(items0_with_attr),
                f"test `apply`: additional attribute {attr_name} is recognized as a field",
            )
            self.assertEqual(
                sorted(chain(default_fields, additional_fields)),
                keys_for_set(items0_with_attr + items1),
                f"test `apply`: additional attribute {attr_name} is preserved after + operation",
            )
            self.assertEqual(
                sorted(chain(default_fields, additional_fields)),
                keys_for_set(items1 + items0_with_attr),
                f"test `apply`: additional attribute {attr_name} is preserved after reversed + operation",
            )
            self.assertEqual(
                attr_value,
                getattr(
                    next(iter(items0_with_attr.select_by_colocation(True))).left,
                    attr_name,
                ),
                f"test `apply`: additional attribute {attr_name} is preserved after set modification",
            )
            self.assertEqual(
                attr_value,
                getattr(
                    next(iter(items0_with_attr.select_by_delay(0, 3600))).left,
                    attr_name,
                ),
                f"test `apply`: additional attribute {attr_name} is preserved after set modification",
            )

    def test_bad_operations(self):
        set0 = [
            ("2000-01-01 01:01", "track1", "device1", 1),
            ("2000-01-01 01:02", "track1", "device1", 1),
        ]

        items = parse_measurements(set0)
        items0 = MeasurementPairSet.from_pairs(
            [CellMeasurementPair(left=items[0], right=items[0])]
        )
        items1 = MeasurementPairSet.from_pairs(
            [CellMeasurementPair(left=items[1], right=items[1])]
        )
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
