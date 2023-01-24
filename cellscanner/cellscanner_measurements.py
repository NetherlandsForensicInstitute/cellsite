import datetime
import logging
from typing import Iterable, Tuple, List, Sequence, Iterator, Optional, Set

from celldb import CellDatabase
from cellscanner.cellscanner_util import resolve_cell
from cellsite import CellMeasurement, WgsPoint, Angle, CellIdentity
from cellsite.measurement import CellMeasurementSet
from cellsite.properties import LocationInfo

LOG = logging.getLogger(__name__)


class CellscannerMeasurementSet(CellMeasurementSet):
    def __init__(
        self,
        con,
        cell_resolver: CellDatabase,
        max_accuracy_m: Optional[int] = None,
        tracks: Optional[List[str]] = None,
        sensors: Optional[List[str]] = None,
        sort_key: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        super().__init__()
        self._con = con
        self.cell_resolver = cell_resolver
        self._max_accuracy_m = max_accuracy_m
        self._selected_tracks = tracks
        self._selected_sensors = sensors
        self._sort_key = sort_key
        self._limit = limit

    def add(self, measurement: CellMeasurement):
        raise NotImplemented()

    def _create_augmented_set(self, **selection) -> CellMeasurementSet:
        augmentations = {
            "max_accuracy_m": self._max_accuracy_m,
            "tracks": self._selected_tracks,
            "sensors": self._selected_sensors,
            "sort_key": self._sort_key,
            "limit": self._limit,
        }
        augmentations.update(selection)
        return CellscannerMeasurementSet(
            con=self._con,
            cell_resolver=self.cell_resolver,
            **selection,
        )

    @property
    def track_names(self) -> List[str]:
        qselect = "DISTINCT device.install_id"
        q, qargs = self._build_query(qselect)
        with self._con.cursor() as cur:
            cur.execute(q, qargs)
            return [row[0] for row in cur]

    @property
    def sensor_names(self) -> List[str]:
        qselect = "DISTINCT cell.subscription"
        q, qargs = self._build_query(qselect)
        with self._con.cursor() as cur:
            cur.execute(q, qargs)
            return [row[0] for row in cur]

    def get_cells(self) -> Set[CellIdentity]:
        cell_fields = ["cell.radio", "cell.mcc", "cell.mnc", "cell.area", "cell.cid"]
        qselect = f"DISTINCT {','.join(cell_fields)}"
        q, qargs = self._build_query(qselect)
        with self._con.cursor() as cur:
            cur.execute(q, qargs)
            return set(
                [
                    CellIdentity.create(
                        radio=radio, mcc=mcc, mnc=mnc, lac=lac, ci=ci, eci=ci
                    )
                    for radio, mcc, mnc, lac, ci in cur
                ]
            )

    def select_by_track(self, *track_names: str) -> CellMeasurementSet:
        return self._create_augmented_set(tracks=track_names)

    def select_by_sensor(self, *sensor_names: str) -> CellMeasurementSet:
        return self._create_augmented_set(sensors=sensor_names)

    def select_by_timestamp(
        self, timestamp_from: datetime.datetime, timestamp_to: datetime.datetime
    ) -> CellMeasurementSet:
        raise NotImplemented()

    def select_by_cell(self, *cells: CellIdentity) -> CellMeasurementSet:
        raise NotImplemented()

    def limit(self, count: int) -> CellMeasurementSet:
        return self._create_augmented_set(limit=count)

    def sort_by(self, key: str) -> CellMeasurementSet:
        return self._create_augmented_set(sort_key=key)

    def select_by_max_accuracy(self, accuracy: int) -> CellMeasurementSet:
        return self._create_augmented_set(max_accuracy_m=accuracy)

    def _build_query(self, qselect: str) -> Tuple[str, Sequence]:
        qwhere = []
        qargs = []

        if self._selected_tracks is not None:
            qwhere.append(
                f"(device.install_id in ({','.join(['%s'] * len(self._selected_tracks))}) OR device.tag in ({','.join(['%s'] * len(self._selected_tracks))}))"
            )
            qargs.extend(self._selected_tracks)
            qargs.extend(self._selected_tracks)

        if self._selected_sensors is not None:
            qwhere.append(
                f"subscription in ({','.join(['%s'] * len(self._selected_sensors))})"
            )
            qargs.extend(self._selected_sensors)

        if self._max_accuracy_m is not None:
            qwhere.append("accuracy <= %s")
            qargs.append(self._max_accuracy_m)

        if len(qwhere) == 0:
            qwhere.append("TRUE")

        qorder = ""
        if self._sort_key is not None:
            qorder = f"ORDER BY {self._sort_key}"

        qlimit = ""
        if self._limit is not None:
            qlimit = f"LIMIT {self._limit}"
            if qorder == "":
                qorder = "ORDER BY random()"

        q = f"""
                SELECT {qselect}
                FROM locationinfo l
                    JOIN device ON l.device_id = device.id
                    JOIN cellinfo cell ON cell.device_id = device.id AND cell.date_start <= l.timestamp AND l.timestamp < cell.date_end
                WHERE {' AND '.join(qwhere)}
                {qorder}
                {qlimit}
            """

        return q, qargs

    def iter_measurements(self) -> Iterable[CellMeasurement]:
        cell_fields = ["radio", "mcc", "mnc", "area", "cid"]
        qselect = f"""
            l.timestamp,
            COALESCE(device.tag, device.install_id) as track_name,
            cell.subscription as device_name,
            l.latitude, l.longitude, l.accuracy, l.speed, l.bearing_deg,
            {','.join(f"cell.{colname}" for colname in cell_fields)}
        """
        q, qargs = self._build_query(qselect)

        with self._con.cursor() as cur:
            cur.execute(q, qargs)
            for (
                timestamp,
                track,
                sensor,
                latitude,
                longitude,
                accuracy,
                speed,
                bearing_deg,
                radio,
                mcc,
                mnc,
                lac,
                ci,
            ) in cur:
                cell, cellinfo = resolve_cell(
                    self.cell_resolver, timestamp, radio, mcc, mnc, lac, ci
                )
                locationinfo = LocationInfo(
                    wgs84=WgsPoint(lat=latitude, lon=longitude),
                    accuracy=accuracy,
                    speed=speed,
                )
                if bearing_deg is not None:
                    locationinfo["bearing"] = Angle(degrees=bearing_deg)
                yield CellMeasurement(
                    timestamp,
                    cell=cell,
                    track=track,
                    sensor=sensor,
                    geo=cellinfo,
                    device_geo=locationinfo,
                )

    def __iter__(self) -> Iterator[CellMeasurement]:
        yield from self.iter_measurements()

    def __len__(self):
        qselect = "COUNT(*)"
        q, qargs = self._build_query(qselect)
        with self._con.cursor() as cur:
            cur.execute(q, qargs)
            return cur.fetchone()[0]
