Library for cell site analysis
==============================

Prepare environment
-------------------

```sh
virtualenv -p python3.10 venv
source venv/bin/activate
pip install -r requirements.in
pre-commit install
```

Test if it runs:

```sh
python -m celldb --help
python -m colocation --help
```

Import cell database
--------------------

In many practical situations you will need a cell database which contains
information on the actual positions of the cell antennas, as well as other
meta data. The following assumes you have such a database in a readable CSV
format.

Install Postgres and Postgis and remember credentials.

Postgis must be added to the database explicitly after installation:

```sql
CREATE EXTENSION postgis;
```

Create `cellsite.yaml` from the template and insert the Postgres credentials.

```sh
cp cellsite.yaml-example cellsite.yaml
nano cellsite.yaml
```

```sh
python -m celldb --config cellsite.yaml import < celldb.csv
```

For more information, see `celldb` documentation.


Import Cellscanner data into postgres
-------------------------------------

Cellscanner is a tool for collecting cell measurements in the field. It
registers the serving cell as well as the GPS coordinates of the device. The
following assumes that you have collected measurements using Cellscanner, or
have access to such a dataset.

Prepare the configuration file `cellscanner.yaml`. Optionally, use the
credentials from the previously created `cellsite.yaml`.

```sh
nano cellscanner.yaml
```

Cellscanner produces Sqlite files. Import these data into postgres.

```sh
python -m cellscanner --cellscanner-config cellscanner.yaml file-to-postgres data/cellscanner/*.sqlite3
```

You may want to inspect the contents of the measurement database.

```sh
python -m cellscanner --cellscanner-config cellscanner.yaml summarize
python -m cellscanner --celldb-config cellsite.yaml --cellscanner-config cellscanner.yaml export-measurements --limit 1000
```


Generate training data
----------------------

```sh
python -m cellscanner \
        --cellscanner-config cellscanner.yaml \
        --celldb-config celldb.yaml \
        --on-duplicate-cell take-first \
    generate-cellscanner-pairs \
        --max-delay 60 \
        --limit-colocated 1000 \
        --limit-dislocated 1000 \
        --write-pairs data/cellscanner_pairs.db
```

Choose parameter values as desired.

The `on-duplicate-cell` policy is relevant if the cell database has two or
more hits for the same cell. This may mean that the cell database is
inconsistant.

Use extension `.csv` instead of `.db` to get CSV output.


Test case: julia
----------------

Form pairs:

```sh
python -m colocation pair-measurements \
    --measurements-file data/julia/measurements.csv \
    --write-pairs data/julia/pairs.db
```

See docs/
