import datetime
import unittest

import pytz

import cellsite
from cellsite import CellMeasurement, CellIdentity
from colocation.serialization import PAIR_SERIALIZERS


class TestPairSerialization(unittest.TestCase):
    def test_collapse_expand(self):
        now = datetime.datetime.now()
        pairs = [
            (
                {"a": "b"},
                {"a": "b"},
            ),
            (
                {"features": {"1": 1.1, "2": 1.2}},
                {"features.1_float": 1.1, "features.2_float": 1.2},
            ),
            (
                {
                    "left": CellMeasurement(
                        timestamp=datetime.datetime(
                            2021,
                            11,
                            6,
                            9,
                            58,
                            20,
                            tzinfo=pytz.timezone("CET"),
                        ),
                        cell=CellIdentity.create(
                            radio="LTE", mcc=262, mnc=1, eci=40002572
                        ),
                        call_id="1351",
                    ),
                    "right": CellMeasurement(
                        timestamp=datetime.datetime(
                            2022,
                            5,
                            10,
                            18,
                            16,
                            2,
                            tzinfo=pytz.timezone("EET"),
                        ),
                        cell=CellIdentity.create(
                            radio="LTE", mcc=262, mnc=2, eci=21055253
                        ),
                        call_id="2366",
                    ),
                    "features": {
                        "measurement_delay": -16329654.46603279,
                        "distance_m": 1457.8343129392676,
                        "cell1_angle": 162.4882857566774,
                        "cell2_angle": -103.80075502717891,
                    },
                    "is_colocated": False,
                },
                {
                    "left_msrmnt.timestamp": "2021-11-06T09:58:20+01:00",
                    "left_msrmnt.cell.radio": "LTE",
                    "left_msrmnt.cell.identifier": "262-1-40002572",
                    "left_msrmnt.call_id": "1351",
                    "right_msrmnt.timestamp": "2022-05-10T18:16:02+02:00",
                    "right_msrmnt.cell.radio": "LTE",
                    "right_msrmnt.cell.identifier": "262-2-21055253",
                    "right_msrmnt.call_id": "2366",
                    "features.distance_m_float": 1457.8343129392676,
                    "features.cell1_angle_float": 162.4882857566774,
                    "features.cell2_angle_float": -103.80075502717891,
                    "features.measurement_delay_float": -16329654.46603279,
                    "is_colocated": False,
                },
            ),
        ]
        for i, (d_full, d_collapsed) in enumerate(pairs):
            self.assertEqual(
                d_collapsed,
                cellsite.serialization._collapse_dict(d_full, PAIR_SERIALIZERS, set()),
                f"test {i}: collapse",
            )
            self.assertEqual(
                d_full,
                cellsite.serialization._expand_dict(d_collapsed, PAIR_SERIALIZERS),
                f"test {i}: expand",
            )
