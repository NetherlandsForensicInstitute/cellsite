import csv
import datetime
import math
import warnings
from typing import Optional, List, Iterable, Callable

from tqdm import tqdm

from cellsite.properties import Properties
from . import duplicate_policy
from .cell_database import CellDatabase
from cellsite import CellIdentity
from cellsite.cell_identity import CellGlobalIdentity, LTECell
from cellsite.coord import Point, RdPoint, WgsPoint


def _build_antenna(row):
    date_start, date_end, radio, mcc, mnc, lac, ci, eci, rdx, rdy, azimuth_degrees = row
    if radio == "GSM" or radio == "UMTS":
        retrieved_ci = CellIdentity.create(radio, mcc, mnc, lac, ci=ci)
    elif radio == "LTE":
        retrieved_ci = CellIdentity.create(radio, mcc, mnc, lac, eci=eci)
    elif radio is None:
        retrieved_ci = CellIdentity.create(radio, mcc, mnc, lac, ci=ci, eci=eci)
    else:
        raise ValueError(f"unrecognized radio type: {radio}")

    coords = RdPoint(rdx, rdy)
    return Properties(wgs84=coords, azimuth_degrees=azimuth_degrees, cell=retrieved_ci)


def _build_cell_identity_query(ci):
    qwhere = []
    qargs = []
    if ci.radio is not None:
        qwhere.append("radio = %s")
        qargs.append(ci.radio)
    if ci.mcc is not None:
        qwhere.append(f"mcc = {ci.mcc}")
    if ci.mnc is not None:
        qwhere.append(f"mnc = {ci.mnc}")

    if isinstance(ci, CellGlobalIdentity):
        if ci.lac is not None:
            qwhere.append(f"lac = {ci.lac}")
        if ci.ci is not None:
            qwhere.append(f"ci = {ci.ci}")

    elif isinstance(ci, LTECell):
        if ci.eci is not None:
            qwhere.append(f"eci = {ci.eci}")

    else:
        raise ValueError(f"unsupported cell type: {type(ci)}")

    return " AND ".join(qwhere), qargs


class PgDatabase(CellDatabase):
    def __init__(
        self,
        con,
        qwhere=None,
        qargs=None,
        qorder=None,
        count_limit: int = None,
        on_duplicate: Callable = duplicate_policy.warn,
    ):
        self._con = con
        self._qwhere = qwhere or []
        self._qargs = qargs or []
        self._qorder = qorder or ""
        self._count_limit = count_limit
        self._cur = None
        self._on_duplicate = on_duplicate

    def get(self, date: datetime.datetime, ci: CellIdentity) -> Optional[Properties]:
        """
        Retrieve a specific antenna from database.

        :param date: Used to select active antennas
        :param ci: The cell identity
        :return: The retrieved antenna or None
        """
        if isinstance(date, datetime.date):
            date = datetime.datetime.combine(date, datetime.datetime.min.time())

        qwhere = self._qwhere + [
            "(date_start is NULL OR %s >= date_start) AND (date_end is NULL OR %s < date_end)"
        ]
        qargs = self._qargs + [date, date]

        add_qwhere, add_qargs = _build_cell_identity_query(ci)
        qwhere.append(add_qwhere)
        qargs.extend(add_qargs)

        with self._con.cursor() as cur:
            cur.execute(
                f"""
                SELECT date_start, date_end, radio, mcc, mnc, lac, ci, eci, ST_X(rd), ST_Y(rd), azimuth
                FROM antenna_light
                WHERE {' AND '.join(qw for qw in qwhere)}
            """,
                qargs,
            )

            results = [_build_antenna(row) for row in cur.fetchall()]
            if len(results) == 0:
                return None
            elif len(results) > 1:
                return self._on_duplicate(ci, results)
            else:
                return results[0]

    def search(
        self,
        coords: Point = None,
        distance_limit_m: float = None,
        distance_lower_limit_m: float = None,
        date: datetime.datetime = None,
        radio: Optional[Iterable[str]] = None,
        mcc: int = None,
        mnc: int = None,
        count_limit: Optional[int] = 10000,
        random_order: bool = False,
        exclude: Optional[List[CellIdentity]] = None,
    ) -> CellDatabase:
        """
        Given a Point, find antennas that are in reach from this point sorted by the distance from the grid point.

        :param coords: Point for which nearby antennas are retrieved
        :param distance_limit_m: antennas should be within this range
        :param date: used to select active antennas
        :param radio: antennas should be limited to this radio technology, e.g.: LTE, UMTS, GSM (accepts `str` or list of `str`)
        :param mcc: antennas should be limited to this mcc
        :param mnc: antennas should be limited to this mnc
        :param count_limit: maximum number of antennas to return
        :param exclude: antenna that should be excluded from the retrieved antennas
        :return: retrieved antennas within reach from the Point
        """
        qwhere = list(self._qwhere)
        qargs = list(self._qargs)
        if coords is not None:
            assert (
                distance_limit_m is not None
            ), "search for coords without distance limit"
            coords = coords.rd()
            qwhere.append(
                f"ST_DWithin(rd, 'SRID=4326;POINT({coords.rd().x} {coords.rd().y})', {distance_limit_m})"
            )
            if distance_lower_limit_m is not None:
                qwhere.append(
                    f"NOT ST_DWithin(rd, 'SRID=4326;POINT({coords.rd().x} {coords.rd().y})', {distance_lower_limit_m})"
                )
        if date is not None:
            qwhere.append("(date_start is NULL OR %s >= date_start)")
            qwhere.append("(date_end is NULL OR %s < date_end)")
            qargs.extend([date, date])

        if radio is not None:
            if isinstance(radio, str):
                radio = [radio]
            qwhere.append(f"({' OR '.join(['radio = %s'])})")
            qargs.extend(radio)

        if mcc is not None:
            qwhere.append(f"mcc = {mcc}")
        if mnc is not None:
            qwhere.append(f"mnc = {mnc}")

        if exclude is not None:
            if isinstance(exclude, CellIdentity):
                exclude = [exclude]
            for addr in exclude:
                add_qwhere, add_qargs = _build_cell_identity_query(addr)
                qwhere.append(f"NOT ({add_qwhere})")
                qargs.extend(add_qargs)

        count_limit = count_limit if count_limit is not None else self._count_limit
        qorder = self._qorder
        if random_order is not None:
            if random_order:
                qorder = "ORDER BY RANDOM()"
            elif coords is not None:
                qorder = f"ORDER BY ST_Distance(rd, 'SRID=4326;POINT({coords.rd().x} {coords.rd().y})')"

        return PgDatabase(
            self._con, qwhere, qargs, qorder, count_limit, self._on_duplicate
        )

    def __enter__(self):
        self._cur = self._con.cursor()
        return self

    def __exit__(self, type, value, tb):
        self._cur.close()

    def __iter__(self):
        assert self._cur is not None, "use within context"

        q = f"""
            SELECT date_start, date_end, radio, mcc, mnc, lac, ci, ST_X(rd), ST_Y(rd), azimuth
            FROM antenna_light
            WHERE {' AND '.join(qw for qw in self._qwhere)}
            {self._qorder}
        """
        if self._count_limit is not None:
            q += f" LIMIT {self._count_limit}"

        self._cur.execute(q, self._qargs)
        return self

    def __next__(self):
        row = self._cur.fetchone()
        if row is None:
            raise StopIteration

        return _build_antenna(row)

    def __len__(self):
        with self._con.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM antenna_light
                WHERE {' AND '.join(qw for qw in self._qwhere)}
            """,
                self._qargs,
            )

            return cur.fetchone()[0]


def csv_import(con, flo, show_progress=True):
    create_table(con)

    reader = csv.reader(flo)
    next(reader)  # skip header

    with con.cursor() as cur:
        for i, row in enumerate(
            tqdm(list(reader), desc="reading cells", disable=not show_progress)
        ):
            try:
                (
                    date_start,
                    date_end,
                    radio,
                    mcc,
                    mnc,
                    lac,
                    ci,
                    eci,
                    lon,
                    lat,
                    azimuth,
                ) = [c if c != "" else None for c in row]
                lon, lat = float(lon), float(lat)
                assert math.isfinite(lon), f"invalid number for longitude: {lon}"
                assert math.isfinite(lat), f"invalid number for latitude: {lat}"
                assert ci is not None or eci is not None

                p = WgsPoint(lon=lon, lat=lat).rd()
                cur.execute(
                    f"""
                    INSERT INTO antenna_light(date_start, date_end, radio, mcc, mnc, lac, ci, eci, rd, azimuth)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, 'SRID=4326;POINT('||%s||' '||%s||')', %s)
                """,
                    (
                        date_start,
                        date_end,
                        radio,
                        mcc,
                        mnc,
                        lac,
                        ci,
                        eci,
                        p.x,
                        p.y,
                        azimuth,
                    ),
                )
                con.commit()
            except Exception as e:
                warnings.warn(f"import error at line {i+2}: {e}")


def csv_export(con, flo):
    sql_x = "ST_X(rd)"
    sql_y = "ST_Y(rd)"
    sql_lon = f"""
         5.38720621 + ((
         (5260.52916 * (({sql_x} - 155000) * 10 ^ -5)) +
         (105.94684 * (({sql_x} - 155000) * 10 ^ -5) * (({sql_y} - 463000) * 10 ^ -5)) +
         (2.45656 * (({sql_x} - 155000) * 10 ^ -5) * (({sql_y} - 463000) * 10 ^ -5) ^ 2) +
         (-0.81885 * (({sql_x} - 155000) * 10 ^ -5) ^ 3) +
         (0.05594 * (({sql_x} - 155000) * 10 ^ -5) * (({sql_y} - 463000) * 10 ^ -5) ^ 3) +
         (-0.05607 * (({sql_x} - 155000) * 10 ^ -5) ^ 3 * (({sql_y} - 463000) * 10 ^ -5)) +
         (0.01199 * (({sql_y} - 463000) * 10 ^ -5)) +
         (-0.00256 * (({sql_x} - 155000) * 10 ^ -5) ^ 3 * (({sql_y} - 463000) * 10 ^ -5) ^ 2) +
         (0.00128 * (({sql_x} - 155000) * 10 ^ -5) * (({sql_y} - 463000) * 10 ^ -5) ^ 4) +
         (0.00022 * (({sql_y} - 463000) * 10 ^ -5) ^ 2) +
         (-0.00022 * (({sql_x} - 155000) * 10 ^ -5) ^ 2) +
         (0.00026 * (({sql_x} - 155000) * 10 ^ -5) ^ 5)
         ) / 3600)
    """
    sql_lat = f"""
         52.15517440 + ((
         (3235.65389 * (({sql_y} - 463000) * 10 ^ -5)) +
         (-32.58297 * (({sql_x} - 155000) * 10 ^ -5) ^ 2) +
         (-0.2475 * (({sql_y} - 463000) * 10 ^ -5) ^ 2) +
         (-0.84978 * (({sql_x} - 155000) * 10 ^ -5) ^ 2 * (({sql_y} - 463000) * 10 ^ -5)) +
         (-0.0655 * (({sql_y} - 463000) * 10 ^ -5) ^ 3) +
         (-0.01709 * (({sql_x} - 155000) * 10 ^ -5) ^ 2 * (({sql_y} - 463000) * 10 ^ -5) ^ 2) +
         (-0.00738 * (({sql_x} - 155000) * 10 ^ -5)) +
         (0.0053 * (({sql_x} - 155000) * 10 ^ -5) ^ 4) +
         (-0.00039 * (({sql_x} - 155000) * 10 ^ -5) ^ 2 * (({sql_y} - 463000) * 10 ^ -5) ^ 3) +
         (0.00033 * (({sql_x} - 155000) * 10 ^ -5) ^ 4 * (({sql_y} - 463000) * 10 ^ -5)) +
         (-0.00012 * (({sql_x} - 155000) * 10 ^ -5) * (({sql_y} - 463000) * 10 ^ -5))
         ) / 3600)
    """
    q = f"""
        SELECT date_start, date_end, radio, mcc, mnc, lac, ci, eci, {sql_lon} lon, {sql_lat} lat, azimuth
        FROM antenna_light
    """
    with con.cursor() as cur:
        cur.copy_expert(f"copy ({q}) to stdout with csv header", flo)


def create_table(con):
    tablename = "antenna_light"
    with con.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {tablename}")
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        cur.execute(
            f"""
            CREATE TABLE {tablename} (
                id SERIAL PRIMARY KEY,
                date_start TIMESTAMP WITH TIME ZONE,
                date_end TIMESTAMP WITH TIME ZONE,
                radio VARCHAR(5) NULL,
                mcc INT NOT NULL,
                mnc INT NOT NULL,
                lac INT NULL,
                ci INT NULL,
                eci INT NULL,
                rd GEOMETRY(point,4326) NOT NULL,
                azimuth INT NULL
            )
        """
        )
        cur.execute(f"CREATE INDEX {tablename}_start ON {tablename}(date_start)")
        cur.execute(f"CREATE INDEX {tablename}_end ON {tablename}(date_end)")
        cur.execute(f"CREATE INDEX {tablename}_cgi ON {tablename}(mcc, mnc, lac, ci)")
        cur.execute(f"CREATE INDEX {tablename}_ecgi ON {tablename}(mcc, mnc, eci)")
        cur.execute(f"CREATE INDEX {tablename}_rd ON {tablename} USING GIST(rd)")
