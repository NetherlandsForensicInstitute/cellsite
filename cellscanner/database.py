import collections
from typing import Iterable, Tuple


Table = collections.namedtuple("Table", ["name", "columns", "indexes"])
Index = collections.namedtuple("Index", ["columns", "unique"])


TABLES = [
    Table(
        "device",
        [
            "id SERIAL PRIMARY KEY",
            "install_id VARCHAR(255) UNIQUE NOT NULL",
            "tag VARCHAR(100) NULL",
        ],
        [
            Index(("install_id",), unique=True),
        ],
    ),
    Table(
        "message",
        [
            "id SERIAL PRIMARY KEY",
            "device_id INT NOT NULL REFERENCES device(id)",
            "date TIMESTAMP WITH TIME ZONE NOT NULL",
            "message VARCHAR(250) NOT NULL",
        ],
        [
            Index(("device_id", "date"), unique=False),
            Index(("date",), unique=False),
        ],
    ),
    Table(
        "ip_traffic",
        [
            "id SERIAL PRIMARY KEY",
            "device_id INT NOT NULL REFERENCES device(id)",
            "date_start TIMESTAMP WITH TIME ZONE NOT NULL",
            "date_end TIMESTAMP WITH TIME ZONE",
            "bytes_read INT NOT NULL",
        ],
        [
            Index(("device_id", "date_start"), unique=True),
            Index(("date_start",), unique=False),
        ],
    ),
    Table(
        "locationinfo",
        [
            "id SERIAL PRIMARY KEY",
            "device_id INT NOT NULL REFERENCES device(id)",
            "timestamp TIMESTAMP WITH TIME ZONE NOT NULL",
            "provider VARCHAR(200)",
            "latitude FLOAT NOT NULL",
            "longitude FLOAT NOT NULL",
            "accuracy INT",
            "altitude INT",
            "altitude_acc INT",
            "speed INT",
            "speed_acc INT",
            "bearing_deg INT",
            "bearing_deg_acc INT",
        ],
        [
            Index(("device_id", "timestamp"), unique=True),
            Index(("timestamp",), unique=False),
            Index(("accuracy",), unique=False),
        ],
    ),
    Table(
        "cellinfo",
        [
            "id SERIAL PRIMARY KEY",
            "device_id INT NOT NULL REFERENCES device(id)",
            "subscription VARCHAR(20) NOT NULL",
            "date_start TIMESTAMP WITH TIME ZONE NOT NULL",
            "date_end TIMESTAMP WITH TIME ZONE NOT NULL",
            "registered INT NOT NULL",
            "radio VARCHAR(10) NOT NULL",
            "mcc INT NOT NULL",
            "mnc INT NOT NULL",
            "area INT NOT NULL",
            "cid INT NOT NULL",
            "bsic INT",
            "arfcn INT",
            "psc INT",
            "uarfcn INT",
            "pci INT",
        ],
        [
            Index(("device_id", "subscription", "date_start"), unique=True),
            Index(("date_start",), unique=False),
        ],
    ),
    Table(
        "call_state",
        [
            "id SERIAL PRIMARY KEY",
            "device_id INT NOT NULL REFERENCES device(id)",
            "date TIMESTAMP WITH TIME ZONE NOT NULL",
            "state VARCHAR(20) NOT NULL",
        ],
        [
            Index(("device_id", "date"), unique=True),
            Index(("date",), unique=False),
        ],
    ),
]


class CellscannerDatabase:
    def __init__(self, con):
        self._con = con

    def create_tables(self, drop_first=False):
        with self._con.cursor() as cur:
            if drop_first:
                cur.execute(f"DROP TABLE IF EXISTS device CASCADE")

            for table in TABLES:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {table.name} ({','.join(table.columns)})"
                )

                for index in table.indexes:
                    cur.execute(
                        f"""CREATE {'unique' if index.unique else ''} INDEX IF NOT EXISTS
                        {table.name}_{'_'.join(index.columns)}
                        ON {table.name}({','.join(index.columns)}
                    )"""
                    )

            self._con.commit()

    def add_device(self, install_id, tag, exist_ok=True):
        if exist_ok:
            with self._con.cursor() as cur:
                cur.execute(
                    "SELECT id, tag FROM device WHERE install_id = %s", (install_id,)
                )
                row = cur.fetchone()
                if row:
                    assert (
                        row[1] == tag
                    ), f"tag ({tag}) of device {install_id} does not match previously assign tag ({row[1]})"
                    return row[0]

        with self._con.cursor() as cur:
            cur.execute(
                """INSERT INTO device(install_id, tag) VALUES(%s, %s) RETURNING id""",
                (install_id, tag),
            )
            self._con.commit()
            return cur.fetchone()[0]

    def add_locationinfo(self, device_id, records: Iterable[dict]):
        with self._con.cursor() as cur:
            for record in records:
                columns, values = zip(*record.items())
                cur.execute(
                    f"""
                    INSERT INTO locationinfo(device_id, {",".join(col for col in columns)})
                    VALUES(%s, {",".join("%s" for col in columns)})
                    ON CONFLICT (device_id, timestamp) DO NOTHING
                """,
                    [device_id] + list(values),
                )
            self._con.commit()

    def add_cellinfo(self, device_id, records: Iterable[dict]):
        with self._con.cursor() as cur:
            for record in records:
                columns, values = zip(*record.items())
                cur.execute(
                    f"""
                    INSERT INTO cellinfo(device_id, {",".join(col for col in columns)})
                    VALUES(%s, {",".join("%s" for col in columns)})
                    ON CONFLICT(device_id, subscription, date_start) DO NOTHING
                """,
                    [device_id] + list(values),
                )
            self._con.commit()
