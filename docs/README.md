# Cellsite

Measures co-location of devices in a cellular network.


## Requirements

1. Python 3.8
2. Postgres + postgis

``sh
sudo apt install python3 postgresql postgis
``


## Setup

1. Make sure the requirements are installed

```shell
pip install -r requirements.in
```
2. Set the database password in your local yaml (in your root directory):

```yaml
database:
  credentials:
    password: ***
```

## Usage

### celldb

See: `celldb/README.md`

## Unit tests

To run unit tests, do

```py
pip install pytest
python3 -m pytest
```

If you are planning to develop, please consider to run the tests
automatically before each commit:

```py
git config core.hooksPath .git-hooks
```
