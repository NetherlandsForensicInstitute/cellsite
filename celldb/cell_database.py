from __future__ import annotations
from abc import abstractmethod
import datetime
from typing import Optional, Tuple, Iterable

from cellsite import CellIdentity, Point, Properties


class CellDatabase(Iterable[Properties]):
    @abstractmethod
    def get(self, date: datetime.datetime, ci: CellIdentity) -> Optional[Properties]:
        """
        Retrieve a specific antenna from database.

        :param date: Used to select active antennas
        :param ci: The cell identity
        :return: The retrieved antenna or None
        """
        raise NotImplementedError

    @abstractmethod
    def search(
        self,
        coords: Point,
        distance_limit_m: float = None,
        distance_lower_limit_m: float = None,
        date: datetime.datetime = None,
        radio: Optional[Tuple[str]] = None,
        mcc: int = None,
        mnc: int = None,
        count_limit: Optional[int] = 10000,
        exclude: Optional[CellIdentity] = None,
    ) -> CellDatabase:
        """
        Given a Point, find antennas that are in reach from this point sorted by the distance from the grid point.

        :param coords: Point for which nearby antennas are retrieved
        :param distance_limit_m: antennas should be within this range
        :param date: used to select active antennas
        :param radio: antennas should be limited to this radio technology, e.g.: LTE, UMTS, GSM
        :param mcc: antennas should be limited to this mcc
        :param mnc: antennas should be limited to this mnc
        :param count_limit: maximum number of antennas to return
        :param exclude: antenna that should be excluded from the retrieved antennas
        :return: retrieved antennas within reach from the Point
        """
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError
