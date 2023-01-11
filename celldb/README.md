celldb
======

Package for using and manipulating a cell database.

The cell database is used to:

* retrieve meta data (coordinates, azimuth, etc) of a cell; and
* search for cells in a particular area.

The cell database is a dependency for several other modules and applications.

Dependencies
------------

* Python3
* Access to a PostgreSQL database with Postgis installed.

Setup
-----

Make sure the Python active environment satisfies the requirements listed in
`requirements.txt`.

Enter the Postgres credentials in a YAML file named `celldb.yaml`, like so:
```yaml
database:
  credentials:
    user: cellsite
    host: localhost
    port: 5432
    database: cellsite
    password: SECRET_PASSWORD
  schema: public
```

The postgres user must be a superuser in order to create the Postgis extension.
Alternatively, create the extension manually before initializing the database.

See command listing:
```commandline
python -m celldb --help
```

which prints the following:

```
Usage: python -m celldb [OPTIONS] COMMAND [ARGS]...

Options:
  --config FILE          Comma-separated list of YAML files with database
                         credentials  [required]
  --drop-schema BOOLEAN  Drop schema before doing anything else
  --help                 Show this message and exit.

Commands:
  export  export the database to CSV
  import  import a CSV file into the database
```

The comma-separated values in the import/export should have the following
columns:
* `date_start` a timestamp marking the beginning of the period when the cell was active
* `date_end` a timestamp marking the end of the period when the cell was active
* `radio` the radio technology (one of: GSM, UMTS, LTE, NR)
* `mcc` the mobile country code (MCC) of the cell
* `mnc` the mobile network code (MNC) of the cell
* `lac` the location area code (LAC) of the cell (applies to GSM/UMTS)
* `ci` the cell identity (CI) of the cell (applies to GSM/UMTS)
* `eci` the e-utran cell identity (ECI) of the cell (applies to LTE/NR)
* `lon` the WGS84 longitude of the cell antenna
* `lat` the WGS84 latitude of the cell antenna
* `azimuth` the azimuth of the cell antenna

Save a database to disk:
```commandline
python -m celldb --config celldb.yaml export >celldb.csv
```

Load a database from disk:
```commandline
python -m celldb --config celldb.yaml import <celldb.csv
```

Python API
----------

Connect to a postgres instance where the database is built or imported.

```py
import datetime

from celldb import PgDatabase
from cellsite import CellIdentity

def connect_to_postgres():
    ...

with connect_to_postgres() as con:
    # Initialize the cell database (must be accessible by this connection).
    db = PgDatabase(con)

    # Create a cell identity instance.
    ci = CellIdentity.create('GSM', 204, 3, 4, 5)

    # See what we have on this cell by retrieving it from the database. Because
    # all queries are time sensitive, we need to supply a timestamp as well. For
    # now, use the current time.
    now = datetime.datetime.now()
    cell_info = db.get(now, ci)
    print(f"the info for {ci} is {cell_info}")

    # Search the database for any antennas that may be near our cell.
    results = db.search(coords=cell_info.coords, date=now, distance_limit_m=5000)

    # The results object behaves like a list and is countable.
    print(f"there are {len(results)} antennas within a range of 5000m from {ci}...")

    # The results object is also a cell database itself, which may be searched
    # again to narrow the search.
    results = results.search(radio='LTE')
    print(f"... of which {len(results)} are LTE")
```

For more information, see the documentation for the abstract class `AntennaDatabase`.
