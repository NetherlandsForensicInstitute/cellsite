import os
import warnings
from contextlib import contextmanager
from functools import partial
from itertools import chain

import click

import celldb
import cellsite
import colocation
from celldb import PgDatabase
from cellscanner.cellscanner_measurements import CellscannerMeasurementSet
from cellsite.serialization import write_measurements_to_csv
from colocation import MeasurementPairSet
from colocation.file import write_pairs_to_file
from .cellscanner_measurement_pairs import CellscannerMeasurementPairGenerator
from cellsite.util import script_helper


@contextmanager
def _open_database(cellscanner_config: str, celldb_config: str, on_duplicate_cell: str):
    on_duplicate = getattr(celldb.duplicate_policy, on_duplicate_cell)
    with script_helper.get_database_connection(celldb_config) as dbcon:
        db = PgDatabase(dbcon, on_duplicate=on_duplicate)
        with script_helper.get_database_connection(cellscanner_config) as cscon:
            yield cscon, db


@click.group()
@click.option(
    "--cellscanner-config",
    metavar="PATH",
    type=click.Path(exists=True),
    required=False,
    help="Comma-separated list of YAML files with database credentials",
)
@click.option(
    "--celldb-config",
    metavar="PATH",
    type=click.Path(exists=True),
    required=True,
    help="Comma-separated list of YAML files with database credentials",
)
@click.option(
    "--on-duplicate-cell",
    metavar="exception|warn|take_first|drop",
    help="policy when finding multiple results for a single cell-id",
    default="drop",
)
@click.pass_context
def cli(ctx, cellscanner_config: str, celldb_config: str, on_duplicate_cell: str):
    ctx.ensure_object(dict)
    script_helper.setup_logging("cellscanner.log", verbosity_offset=0)
    ctx.obj["open_database"] = partial(
        _open_database, cellscanner_config, celldb_config, on_duplicate_cell
    )


@cli.command(help="combines cellscanner measurements into pairs")
@click.option(
    "--output-file",
    metavar="PATH",
    type=click.Path(),
    default="-",
    help="where to write measurements",
)
@click.option(
    "--limit",
    metavar="N",
    type=int,
    help="limit the number of measurements by taking a random sample",
)
@click.option(
    "--max-accuracy",
    metavar="METERS",
    type=int,
    help="select measurements with accuracy radius less than METERS",
    default=None,
)
@click.pass_context
def export_measurements(ctx, output_file: str, limit: int, max_accuracy: int):
    with ctx.obj["open_database"]() as (cellscanner_connection, cell_database):
        measurements = CellscannerMeasurementSet(cellscanner_connection, cell_database)
        if limit is not None:
            measurements = measurements.limit(limit)
        if max_accuracy is not None:
            measurements = measurements.select_by_max_accuracy(max_accuracy)

        if os.path.exists(output_file):
            os.remove(output_file)
        write_measurements_to_csv(output_file, measurements)


@cli.command(help="prints a summary of the database contents")
@click.pass_context
def summarize(ctx):
    with ctx.obj["open_database"]() as (cellscanner_connection, cell_database):
        measurements = CellscannerMeasurementSet(cellscanner_connection, cell_database)
        for track in measurements.track_names:
            for device in measurements.select_by_track(track).device_names:
                print(f"{track} {device}")


@cli.command(help="combines cellscanner measurements into pairs")
@click.option(
    "--max-delay",
    metavar="SECONDS",
    help="maximum delay between measurements (seconds)",
    default=0,
)
@click.option(
    "--limit-colocated",
    metavar="N",
    help="maximum number of colocated pairs",
    default=1000,
)
@click.option(
    "--limit-dislocated",
    metavar="N",
    help="maximum number of dislocated pairs",
    default=1000,
)
@click.option(
    "--write-pairs",
    metavar="PATH",
    type=click.Path(),
    help="write measurements to file",
)
@click.pass_context
def generate_cellscanner_pairs(
    ctx, max_delay: int, limit_colocated: int, limit_dislocated: int, write_pairs: str
):
    with ctx.obj["open_database"]() as (cellscanner_connection, cell_database):
        pair_generator = CellscannerMeasurementPairGenerator(
            cellscanner_connection, cell_database
        )
        colocated_pairs = MeasurementPairSet.from_pairs(
            pair_generator.get_colocated_pairs(
                delay_range=(0, max_delay), limit=limit_colocated
            )
        ).with_value(is_colocated=True)
        dislocated_pairs = MeasurementPairSet.from_pairs(
            pair_generator.get_dislocated_pairs(limit=limit_dislocated)
        ).with_value(is_colocated=False)
        if os.path.exists(write_pairs):
            os.remove(write_pairs)
        write_pairs_to_file(write_pairs, colocated_pairs + dislocated_pairs)


if __name__ == "__main__":
    warnings.filterwarnings("error")
    cli()
