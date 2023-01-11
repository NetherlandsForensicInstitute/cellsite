import contextlib
import os
import sqlite3
from typing import Any, Iterable

from cellsite.serialization import _read_dictable_from_csv, _write_dictable_to_csv
from colocation import CellMeasurementPair, MeasurementPairSet
from colocation.measurement_pair_sqlite import SqliteCellMeasurementPairSet
from colocation.serialization import PAIR_SERIALIZERS


class MeasurementPairFileCSV:
    name = "csv"
    extension = "csv"

    @staticmethod
    @contextlib.contextmanager
    def read(file: Any):
        yield MeasurementPairSet.from_pairs(
            _read_dictable_from_csv(
                file,
                None,
                PAIR_SERIALIZERS,
                CellMeasurementPair,
            )
        )

    @staticmethod
    def write(file: Any, pairs: Iterable[CellMeasurementPair], blacklist_types: set):
        _write_dictable_to_csv(file, pairs, PAIR_SERIALIZERS, blacklist_types)


class MeasurementPairFileSqlite:
    name = "sqlite"
    extension = "db"

    @staticmethod
    @contextlib.contextmanager
    def read(file: Any):
        with sqlite3.connect(file) as con:
            yield SqliteCellMeasurementPairSet(sqlite_args={"con": con})

    @staticmethod
    def write(file: Any, pairs: Iterable[CellMeasurementPair], blacklist_types: set):
        if os.path.exists(file):
            os.remove(file)
        with sqlite3.connect(file) as con:
            SqliteCellMeasurementPairSet(
                items=pairs, blacklist_types=blacklist_types, sqlite_args={"con": con}
            )


FILE_TYPES = [
    MeasurementPairFileCSV(),
    MeasurementPairFileSqlite(),
]


def guess_file_format(file: Any, file_type: str):
    """
    Guesses file format.

    @param file: a filename (`str`) or file-like object or the special filename `'-'` (standard input).
    @param file_type: a file type indicator (`str`) or the special value `'guess'`
    @return: a file type object
    """
    if file_type == "guess":
        if isinstance(file, str):
            extension = file.split(".")[-1]
            extension_map = dict(
                (file_type.extension, file_type) for file_type in FILE_TYPES
            )
            if extension not in extension_map:
                raise ValueError(f"unrecognized file extension: {extension}")

            return extension_map[extension]
        else:
            return MeasurementPairFileCSV
    else:
        name_map = dict((file_type.name, file_type) for file_type in FILE_TYPES)
        if file_type not in name_map:
            raise ValueError(f"unrecognized file type: {file_type}")

        return name_map[file_type]


def open_pairs_file(filename: str, file_format: str = "guess"):
    """
    Reads measurement pairs from a file.

    @param filename: a filename (`str`) or file-like object or the special filename `'-'` (standard input).
    @param file_format: a file format indicator or the special value `'guess'`
    @return: a `MeasurementPairSet`
    """
    return guess_file_format(filename, file_format).read(filename)


def write_pairs_to_file(
    file: Any,
    pairs: Iterable[CellMeasurementPair],
    file_format: str = "guess",
    blacklist_types=None,
):
    """
    Writes measurement pairs to a file.

    This method optionally receives a set of value types to be blacklisted. This prevents these types of data to be
    written.

    @param file: a filename (`str`) or file-like object or the special filename `'-'` (standard input).
    @param pairs: the measurement pairs to be written (iterable)
    @param file_format: a file format indicator or the special value `'guess'`
    @param blacklist_types: value types to be blacklisted.
    """
    if blacklist_types is None:
        blacklist_types = set()
    guess_file_format(file, file_format).write(file, pairs, blacklist_types)
