import logging
from typing import Iterable, Tuple

from celldb import CellDatabase
from cellscanner.cellscanner_util import create_cell
from cellsite import CellMeasurement
from colocation.measurement_pair import CellMeasurementPair

LOG = logging.getLogger(__name__)


class CellscannerMeasurementPairGenerator:
    def __init__(self, con, cell_resolver: CellDatabase, seed=None, **kwargs):
        """
        Retrieves co-occurring cell measurements from different tracks of the same device.

        @param con: a connection to a postgres database with access to Cellscanner tables.
        @param cell_resolver: a cell database.
        """
        super().__init__(**kwargs)
        self._con = con
        self.cell_resolver = cell_resolver
        self._seed = seed

    def _fetch_rows(self, rows, is_colocated: bool):
        for row in rows:
            timestamp1 = row[0]
            ci1, geo1 = create_cell(self.cell_resolver, timestamp1, *row[1:6])
            timestamp2 = row[6]
            ci2, geo2 = create_cell(self.cell_resolver, timestamp2, *row[7:12])
            if geo1 is not None and geo2 is not None:
                measurement1 = CellMeasurement(timestamp1, ci1, geo=geo1)
                measurement2 = CellMeasurement(timestamp2, ci2, geo=geo2)
                yield CellMeasurementPair(
                    measurement1, measurement2, is_colocated=is_colocated
                )

    def get_colocated_pairs(
        self, delay_range: Tuple[int, int], limit: int
    ) -> Iterable[CellMeasurementPair]:
        sql_random_log = "(exp(random())-1) / (exp(1)-1)"  # generates a random number in range 0..1 on a log scale
        CELL_COLUMNS = ["radio", "mcc", "mnc", "area", "cid"]
        delay_min, delay_max = delay_range
        with self._con.cursor() as cur:
            cur.execute(
                "CREATE TEMPORARY TABLE IF NOT EXISTS locationinfo_rnd (locationinfo_id INT NOT NULL, delayed_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, rnd FLOAT NOT NULL)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS locationinfo_rnd_index ON locationinfo_rnd(rnd)"
            )
            cur.execute(
                f"""INSERT INTO locationinfo_rnd
                    SELECT locationinfo.id as locationinfo_id,
                        locationinfo.timestamp + interval '{delay_min} seconds' + {sql_random_log} * interval '{delay_max - delay_min} seconds',
                        RANDOM() as rnd
                    FROM locationinfo
            """
            )

            qlimit = ""
            if limit is not None:
                qlimit = f"LIMIT {limit}"

            cur.execute(
                f"""
                SELECT
                    l.timestamp,
                    {','.join(f"cell1.{colname} cell1_{colname}" for colname in CELL_COLUMNS)},
                    rnd.delayed_timestamp,
                    {','.join(f"cell2.{colname} cell2_{colname}" for colname in CELL_COLUMNS)}
                FROM locationinfo l
                    JOIN locationinfo_rnd rnd ON rnd.locationinfo_id = l.id
                    JOIN cellinfo cell1 ON cell1.device_id = l.device_id AND delayed_timestamp >= cell1.date_start AND delayed_timestamp < cell1.date_end
                    JOIN cellinfo cell2 ON cell2.device_id = l.device_id AND cell1.subscription != cell2.subscription AND l.timestamp >= cell2.date_start AND l.timestamp < cell2.date_end
                ORDER BY rnd.rnd
                {qlimit}
            """
            )

            yield from self._fetch_rows(cur.fetchall(), is_colocated=True)

    def get_dislocated_pairs(self, limit: int) -> Iterable[CellMeasurementPair]:
        CELL_COLUMNS = ["radio", "mcc", "mnc", "area", "cid"]
        with self._con.cursor() as cur:
            cur.execute(
                "CREATE TEMPORARY TABLE IF NOT EXISTS cellinfo_rnd (cellinfo_id INT NOT NULL, rnd FLOAT NOT NULL)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS cellinfo_rnd_index ON cellinfo_rnd(rnd)"
            )
            cur.execute(
                f"""INSERT INTO cellinfo_rnd
                    SELECT cellinfo.id as cellinfo_id,
                        RANDOM() as rnd
                    FROM cellinfo
            """
            )

            qlimit = ""
            if limit is not None:
                qlimit = f"LIMIT {limit}"

            # create table `cell_pair` containing pairs of cells of the same device, but ordered randomly
            cur.execute(
                f"""
                WITH cell_pair AS (
                    SELECT
                        cell.id AS cell1_id,
                        LAG(cell.id, 1) OVER (ORDER BY device_id, rnd.rnd) AS cell2_id
                    FROM cellinfo cell
                        JOIN cellinfo_rnd rnd ON rnd.cellinfo_id = cell.id
                    ORDER BY rnd.rnd
                )
                SELECT
                    cell1.date_start,
                    {','.join(f"cell1.{colname} cell1_{colname}" for colname in CELL_COLUMNS)},
                    cell1.date_start,
                    {','.join(f"cell2.{colname} cell2_{colname}" for colname in CELL_COLUMNS)}
                FROM cell_pair pair
                    JOIN cellinfo cell1 ON cell1.id = pair.cell1_id
                    JOIN cellinfo cell2 ON cell2.id = pair.cell2_id
                WHERE cell1.device_id = cell2.device_id
                {qlimit}
            """
            )

            yield from self._fetch_rows(cur.fetchall(), is_colocated=False)
