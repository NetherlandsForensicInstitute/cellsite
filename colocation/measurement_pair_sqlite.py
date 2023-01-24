import datetime
from typing import Optional, Iterable, Set

from cellsite import CellMeasurement
from cellsite.collection_sqlite import SqliteCollection
from colocation import MeasurementPairSet, CellMeasurementPair, serialization


class SqliteCellMeasurementPairSet(SqliteCollection, MeasurementPairSet):
    """
    Measurement pair set with sqlite backend.
    """

    @property
    def track_names(self) -> Set[str]:
        return self.get_unique_values("left_msrmnt.track").union(
            self.get_unique_values("right_msrmnt.track")
        )

    @property
    def sensor_names(self) -> Set[str]:
        return self.get_unique_values("left_msrmnt.sensor").union(
            self.get_unique_values("right_msrmnt.sensor")
        )

    def select_by_left_timestamp(
        self, timestamp_from: datetime.datetime, timestamp_to: datetime.datetime
    ) -> MeasurementPairSet:
        return self.select_by_range(
            **{"left_msrmnt.timestamp": (timestamp_from, timestamp_to)}
        )

    def select_by_left_sensor(self, sensor_name: str) -> MeasurementPairSet:
        return self.select_by_value(**{"left_msrmnt.sensor": sensor_name})

    def select_by_right_sensor(self, sensor_name: str) -> MeasurementPairSet:
        return self.select_by_value(**{"right_msrmnt.sensor": sensor_name})

    def select_by_delay(
        self, delay_min_secs: float, delay_max_secs: float
    ) -> MeasurementPairSet:
        return self.select_by_range(delay_timedelta=(delay_min_secs, delay_max_secs))

    def __init__(
        self,
        items: Optional[Iterable[CellMeasurementPair]] = None,
        blacklist_types=None,
        sqlite_args: dict = None,
    ):
        if blacklist_types is None:
            self.blacklist_types = {}
        else:
            self.blacklist_types = blacklist_types
        default_sqlite_args = {
            "table_name": "measurement_pair",
        }
        sqlite_args = (
            default_sqlite_args
            if sqlite_args is None
            else default_sqlite_args | sqlite_args
        )
        super().__init__(
            items=items,
            **sqlite_args,
        )
        MeasurementPairSet.__init__(self)

    def create_collection(
        self,
        items: Optional[Iterable[CellMeasurement]] = None,
        sqlite_args: dict = None,
    ) -> MeasurementPairSet:
        return SqliteCellMeasurementPairSet(items=items, sqlite_args=sqlite_args)

    def serialize_item(self, item: CellMeasurementPair) -> dict:
        return serialization.serialize_cell_measurement_pair(
            item, blacklist_types=self.blacklist_types
        ) | {"delay_timedelta": item.get_delay()}

    def deserialize_item(self, item: dict[str, str]) -> CellMeasurementPair:
        if "delay_timedelta" in item:
            del item["delay_timedelta"]
        return serialization.deserialize_cell_measurement_pair(item)

    def select_by_colocation(self, is_colocated: bool) -> MeasurementPairSet:
        return self.select_by_value(is_colocated=is_colocated)
