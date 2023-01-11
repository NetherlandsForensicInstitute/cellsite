import unittest
from datetime import datetime
from typing import Sequence

import pytest

from cellsite import CellIdentity, RdPoint, Properties


@pytest.mark.skip(reason="Dependent on external data source")
class AntennaPostgresDatabaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.antenna_db = None

    def _get(
        self, year=2020, month=1, day=1, mcc=204, mnc=4, lac=664, ci=None, eci=None
    ) -> Properties:
        return self.antenna_db.get(
            datetime(year, month, day),
            CellIdentity.create(None, mcc, mnc, lac, ci, eci),
        )

    def test_get(self):
        self.assertIsNotNone(self._get(eci=26436619))
        self.assertIsNotNone(self._get(ci=1))
        self.assertIsNone(self._get(eci=9999999))
        self.assertIsNone(self._get(ci=99999999))
        self.assertIsNone(self._get(year=1900, ci=1))
        self.assertRaises(ValueError, lambda: self._get(mcc=999, ci=1))
        self.assertIsNone(self._get(mnc=9999999, ci=1))
        self.assertIsNone(self._get(lac=9999999, ci=1))

    def test_tele2_merge(self):
        self.assertIsNotNone(self._get(mnc=16, eci=118054677))

    def test_join_on_area_and_lac(self):
        self.assertIsNotNone(self._get(mnc=8, lac=6300, ci=16443))

    def test_start_date_fix(self):
        self.assertIsNotNone(self._get(mnc=16, lac=1510, ci=18356))

    def test_end_date_fix(self):
        self.assertIsNotNone(self._get(mnc=8, eci=758049, year=2019, month=9, day=10))
        self.assertIsNone(self._get(mnc=8, eci=758049, year=2019, month=9, day=20))
        self.assertIsNotNone(self._get(mnc=8, eci=758049, year=2019, month=10, day=1))

    def _get_by_coords(
        self,
        coords,
        distance_limit=1000,
        year=2020,
        radio="LTE",
        mcc=204,
        mnc=4,
        count_limit=100,
        excluded=None,
    ) -> Sequence[Properties]:
        return self.antenna_db.search(
            coords,
            distance_limit_m=distance_limit,
            date=datetime(year, 1, 1),
            radio=radio,
            mcc=mcc,
            mnc=mnc,
            count_limit=count_limit,
            exclude=excluded,
        )

    def test_get_by_coords(self):
        coords = RdPoint(176603, 315801)
        antennas = self._get_by_coords(coords)
        self.assertEqual(9, len(antennas))
        distances = list(euclidean_distance(coords, a.coords) for a in antennas)
        self.assertListEqual(distances, sorted(distances))
        self.assertTrue(all(dis < 1000 for dis in distances))
        self.assertFalse(self._get_by_coords(coords.move(10, 10), distance_limit=1))
        self.assertFalse(self._get_by_coords(coords, year=1990))
        self.assertRaises(ValueError, lambda: self._get_by_coords(coords, radio="6G"))
        self.assertRaises(ValueError, lambda: self._get_by_coords(coords, mcc=999))
        self.assertFalse(self._get_by_coords(coords, mnc=123))
        self.assertEqual(3, len(self._get_by_coords(coords, count_limit=3)))
        self.assertNotEqual(
            antennas[0], self._get_by_coords(coords, excluded=antennas[0])[0]
        )

    def test_get_by_coords_no_warning(self):
        with pytest.warns(None) as record:
            _ = self._get_by_coords(
                RdPoint(95515, 436153), radio=("LTE",), mnc=8, distance_limit=3000
            )
            assert len(record) == 0

    def test_get_by_coords_warning_too_few(self):
        with self.assertLogs(level="WARNING") as cm:
            _ = self._get_by_coords(
                RdPoint(114260, 507710), radio=("LTE",), mnc=8, distance_limit=3000
            )
            self.assertTrue("Found 3 antennas in area" in "".join(cm.output))

    def test_get_by_coords_warning_too_many(self):
        with self.assertLogs(level="WARNING") as cm:
            _ = self._get_by_coords(
                RdPoint(120556, 485810), radio=("LTE",), mnc=16, distance_limit=3000
            )
            self.assertTrue("Found 2165 antennas in area" in "".join(cm.output))

    def test_count(self):
        self.assertEqual(1679828, self.antenna_db.count())
        self.assertEqual(313009, self.antenna_db.count(datetime(2020, 1, 1)))
        self.assertEqual(0, self.antenna_db.count(datetime(1900, 1, 1)))

    def test_invalid_azimuth(self):
        self.assertIsNone(self._get(year=2019, lac=42002, ci=27447))

    def test_get_antenna_from_database(self):
        antenna = get_antenna_from_database(
            self.antenna_db,
            mcc=204,
            mnc=8,
            lac=6340,
            cid=23934,
            date=datetime(2020, 3, 26),
        )
        self.assertIsNotNone(antenna)

        with self.assertLogs(level="INFO") as cm:
            _ = get_antenna_from_database(
                self.antenna_db,
                mcc=204,
                mnc=8,
                lac=-1,
                cid=-1,
                date=datetime(2020, 3, 26),
            )
            self.assertTrue(
                "returned no antenna results from antenna database"
                in "".join(cm.output)
            )

        with self.assertLogs(level="INFO") as cm:
            _ = get_antenna_from_database(
                self.antenna_db,
                mcc=204,
                mnc=8,
                lac=3190,
                cid=10529,
                date=datetime(2020, 3, 26),
            )
            self.assertTrue(
                "returned multiple antenna results from antenna database"
                in "".join(cm.output)
            )
