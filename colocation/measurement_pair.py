from __future__ import annotations
import datetime
from abc import abstractmethod
from itertools import chain, combinations
from typing import (
    Iterable,
    Tuple,
    Iterator,
    Sized,
    Callable,
)

from cellsite import CellMeasurement, CellMeasurementSet


class CellMeasurementPair:
    """
    An instance of a pair of cell measurements.

    A cell measurement pair has a `left` cell, a `right` cell, and optionally other values specified during
    construction.
    """

    def __init__(self, left: CellMeasurement, right: CellMeasurement, **extra):
        assert isinstance(left, CellMeasurement), "left must have type `CellMeasurement"
        assert isinstance(
            right, CellMeasurement
        ), "right must have type `CellMeasurement"
        self.left = left
        self.right = right
        self.extra = extra

    def get_delay(self) -> datetime.timedelta:
        """
        Calculates the time between the two measurements in the pair.

        @return: the time between the two measurements
        """
        if self.right.timestamp > self.left.timestamp:
            return self.right.timestamp - self.left.timestamp
        else:
            return self.left.timestamp - self.right.timestamp

    def as_dict(self):
        return {
            "left": self.left,
            "right": self.right,
        } | self.extra

    def with_value(self, **values) -> CellMeasurementPair:
        d = self.as_dict()
        for key, value in values.items():
            if callable(value):
                value = value(self)
            d[key] = value
        return CellMeasurementPair(**d)

    def __getattr__(self, item):
        if item == "delay":
            return self.get_delay()
        elif item in self.extra:
            return self.extra[item]
        else:
            raise AttributeError(item)

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and self.left == other.left
            and self.right == other.right
            and self.extra == other.extra
        )

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return f"CellMeasurementPair({self.left}, {self.right}, {self.extra})"


class MeasurementPairSet(Iterable[CellMeasurementPair], Sized):
    """
    Set of measurement pairs, abstract class.

    Can be used as an interator of cell measurement pairs.

    The class contains various methods to reduce the set by certain criteria, or to order the results. These methods
    return a modified instance of the set.
    """

    @property
    @abstractmethod
    def track_names(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def sensor_names(self):
        raise NotImplementedError

    @abstractmethod
    def select_by_left_timestamp(
        self, timestamp_from: datetime.datetime, timestamp_to: datetime.datetime
    ) -> MeasurementPairSet:
        """
        Select measurement pairs by timestamp of left measurement.

        @param timestamp_from: the lower bound (inclusive)
        @param timestamp_to: the upper bound (exclusive)
        @return: an augmented version of this set with only pairs which satisfy timestamp constraints
        """
        raise NotImplementedError

    @abstractmethod
    def select_by_left_sensor(self, sensor_name: str) -> MeasurementPairSet:
        raise NotImplementedError

    @abstractmethod
    def select_by_right_sensor(self, sensor_name: str) -> MeasurementPairSet:
        raise NotImplementedError

    @abstractmethod
    def select_by_delay(
        self, delay_min_secs: float, delay_max_secs: float
    ) -> MeasurementPairSet:
        """
        Select measurement pairs by delay.

        @param delay_min_secs: the minimum number of seconds between measurements in a pair
        @param delay_max_secs: the maximum number of seconds between measurements in a pair
        @return: an augmented version of this set with only pairs which satisfy delay constraints
        """
        raise NotImplementedError

    @abstractmethod
    def select_by_colocation(self, is_colocated: bool) -> MeasurementPairSet:
        """
        Select measurement pairs by colocation status.

        @param is_colocated: wether to keep colocated or dislocated pairs
        @return: an augmented version of this set with only colocated or only dislocated pairs
        """
        raise NotImplementedError

    @abstractmethod
    def limit(self, limit: int) -> MeasurementPairSet:
        """
        Limits the number of pairs in the returned set.

        @param limit: the maximum number of returned pairs
        @return: an augmented version of this set with a limited number of pairs
        """
        raise NotImplementedError

    @abstractmethod
    def sort_by(self, key: str) -> MeasurementPairSet:
        """
        Sort pairs by the specified key.

        @param key: an SQL expression to use for sorting
        @return: an augmented version of this set with sorted pairs
        """
        raise NotImplementedError

    def add(self, pair: CellMeasurementPair):
        raise NotImplementedError

    def with_value(self, **values) -> MeasurementPairSet:
        return MeasurementPairSet.from_pairs(pair.with_value(**values) for pair in self)

    def apply(self, function: Callable):
        return MeasurementPairSet.from_pairs(function(pair) for pair in self)

    @staticmethod
    def create() -> MeasurementPairSet:
        return MeasurementPairSet.from_pairs([])

    @staticmethod
    def from_pairs(pairs: Iterable[CellMeasurementPair]) -> MeasurementPairSet:
        from colocation.measurement_pair_sqlite import SqliteCellMeasurementPairSet

        return SqliteCellMeasurementPairSet(pairs)

    @staticmethod
    def from_random_measurements_within_track(
        measurements: CellMeasurementSet,
    ) -> MeasurementPairSet:
        """
        Constructs a set of pairs from random cell measurements.

        Only measurements of sensors in the same track are eligible for pairing.

        @param measurements: measurements with a `track` and a `sensor` attribute
        @return: a set of measurement pairs
        """
        pair_fields = {"is_colocated": False}
        pairs = chain(
            *(
                pair_sequential_measurements(
                    measurements.select_by_track(track_name),
                    sort_key="random()",
                    pair_fields=pair_fields,
                )
                for track_name in measurements.track_names
            )
        )
        return MeasurementPairSet.from_pairs(pairs)

    @staticmethod
    def from_sequential_measurements(
        measurements: CellMeasurementSet,
        within_track: bool = True,
    ) -> MeasurementPairSet:
        """
        Constructs a set of pairs from cell measurements of colocated sensors.

        Measurements are assumed to have a `track` attribute (e.g. a vehicle or user or something else with a geographic
        position) and a `sensor` attribute (i.e. the source of the measurement). Sensors in the same track are said to
        be colocated. For each two sensors, measurement pairs are constructed by ordering measurements by `timestamp`
        and returning each two consecutive measurements as a pair. If `within_track` is `True`, only sensors of the
        same track are paired.

        @param measurements: measurements with a `track` and a `sensor` attribute
        @param within_track:
        @return: a set of measurement pairs
        """
        if within_track:
            pair_fields = {"is_colocated": True}
            pairs = chain(
                *(
                    pair_sequential_measurements(
                        measurements.select_by_track(track_name),
                        sort_key="timestamp",
                        pair_fields=pair_fields,
                    )
                    for track_name in measurements.track_names
                )
            )
        else:

            def pair_fields(m0: CellMeasurement, m1: CellMeasurement):
                return {"is_colocated": m0.track == m1.track}

            pairs = pair_sequential_measurements(
                measurements, sort_key="timestamp", pair_fields=pair_fields
            )

        return MeasurementPairSet.from_pairs(pairs)

    def __add__(self, other):
        pairs = MeasurementPairSet.from_pairs(self)
        pairs += other
        return pairs

    def __iadd__(self, other):
        for m in other:
            self.add(m)
        return self


def is_duration_within_range(duration, range_min_secs, range_max_secs):
    if range_max_secs is not None and range_max_secs == range_min_secs:
        range_max_secs = range_min_secs + 1
    if range_min_secs is not None and abs(duration.total_seconds()) < range_min_secs:
        return False
    if range_max_secs is not None and abs(duration.total_seconds()) >= range_max_secs:
        return False
    return True


def _get_lag_pairs(
    items: Iterable[CellMeasurement],
) -> Iterable[Tuple[CellMeasurement, CellMeasurement]]:
    prev = None
    for item in items:
        if prev is not None:
            yield sorted(
                [prev, item], key=lambda m: m.sensor
            )  # sort the pair by sensor name
        prev = item


def pair_sequential_measurements(
    measurements: CellMeasurementSet, sort_key: str, pair_fields
) -> Iterator[CellMeasurementPair]:
    for sensor1, sensor2 in combinations(measurements.sensor_names, 2):
        sensor_measurements = measurements.select_by_sensor(sensor1, sensor2).sort_by(
            sort_key
        )
        for pair in _get_lag_pairs(sensor_measurements):
            if pair[0].sensor == pair[1].sensor:
                continue  # pair does not qualify for comparison because it is from the same sensor
            pair_extra = pair_fields(*pair) if callable(pair_fields) else pair_fields
            yield CellMeasurementPair(*pair, **pair_extra)
