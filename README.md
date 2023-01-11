Library for cell site analysis
==============================

Prepare environment
-------------------

```sh
virtualenv -p python3.9 venv
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

Install Postgres and Postgis and remember credentials.

Postgis must be added to the database explicitly after installation:

```sql
CREATE EXTENSION postgis;
```

```sh
cp postgres.yaml.in celldb.yaml
```

Vul configuratiebestand `celldb.yaml` aan met wachtwoord en zo nodig andere
inloggegevens.

```sh
python -m celldb --config celldb.yaml import < celldb.csv
```

Import Cellscanner data into postgres
-------------------------------------

Use the `cellscanner` repository.

```sh
git clone git@github.com:NetherlandsForensicInstitute/cellscanner.git
cd cellscanner/scripts
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```

Je kan de `celldb.yaml` van hierboven hergebruiken.

```sh
cp celldb.yaml cellscanner.yaml
```

Nu is het tijd om de cellscanner-gegevens in te lezen. Daarvoor kan je een
ander schema gebruiken, ik gebruik `cellscanner`. Wijzig dit in `cellscanner.yaml`.

```sh
./load.py data/cellscanner/*.sqlite3
```

Generate training data
----------------------

```sh
python -m colocation generate-cellscanner-pairs \
    --cellscanner-config cellscanner.yaml \
    --celldb-config celldb.yaml \
    --on-duplicate-cell take_first \
    --max-delay 60 \
    --limit-colocated 1000 \
    --limit-dislocated 1000 \
    --write-pairs data/cellscanner_pairs.db
```

Choose parameter values as desired.

The `on-duplicate-cell` policy is relevant if the cell database has two or
more hits for the same cell. This may mean that the cell database is
inconsistant.

Test case: julia
----------------

Form pairs:

```sh
python -m colocation pair-measurements \
    --measurements-file data/julia/measurements.csv \
    --write-pairs data/julia/pairs.db
```

See docs/
