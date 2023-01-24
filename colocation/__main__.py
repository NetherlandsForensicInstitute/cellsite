#!/usr/bin/env python3
import datetime
import math
import os
import sys
import warnings
from collections import namedtuple
from contextlib import contextmanager
from functools import partial
from typing import Tuple, Optional, Iterable, Sequence, List, Any

import click

import lir
import numpy as np
from sklearn.linear_model import LogisticRegression
import sklearn.model_selection
from sklearn.pipeline import Pipeline
from tabulate import tabulate
from tqdm import tqdm

import cellsite
from cellsite.util import script_helper, evaluation
from colocation.file import write_pairs_to_file, open_pairs_file
from .datasplits import DataWithBackgroundCells

from . import (
    CellMeasurementPair,
    MeasurementPairSet,
    triage,
    FeatureExtractor,
    features,
)


PAIRS_SORT_KEY = '"left_msrmnt.timestamp", "left_msrmnt.sensor", "right_msrmnt.timestamp", "right_msrmnt.sensor"'


class ValidationResults(
    namedtuple(
        "ValidationResults", ["parameters", "pairs", "lrs", "labels", "feature_labels"]
    )
):
    def write_results(self, path: str):
        enriched_pairs = MeasurementPairSet.create()
        for pair, lr in zip(self.pairs, self.lrs):
            coords1 = " ".join(str(v) for v in pair.left.geo.wgs84.wgs84().lonlat)
            coords2 = " ".join(str(v) for v in pair.right.geo.wgs84.wgs84().lonlat)

            pair = pair.as_dict() | {
                "loglr": math.log10(lr),
                "cllr": math.log10(1 / lr + 1)
                if pair.is_colocated
                else math.log10(lr + 1),
                "wkt_points": f"MULTIPOINT ({coords1}, {coords2})",
                "wkt_lines": f"LINESTRING ({coords1}, {coords2})",
            }
            enriched_pairs.add(CellMeasurementPair(**pair))

        write_pairs_to_file(
            path,
            tqdm(
                enriched_pairs.sort_by(PAIRS_SORT_KEY), desc=f"writing output to {path}"
            ),
        )

    def write_pav(self, path):
        print(f"writing PAV to {path}")
        with lir.plotting.savefig(path) as ax:
            ax.pav(self.lrs, self.labels)

    def write_lr_histogram(self, path):
        print(f"writing LR histogram to {path}")
        with lir.plotting.savefig(path) as ax:
            ax.lr_histogram(self.lrs, self.labels)

    probability_ranges = [
        ("extreem veel waarschijnlijker", 1e-5),
        ("veel waarschijnlijkers", 0.01),
        ("waarschijnlijker", 0.1),
        ("iets waarschijnlijker", 0.5),
        ("ongeveer even waarschijnlijk", 2),
        ("iets waarschijnlijker", 10),
        ("waarschijnlijker", 100),
        ("veel waarschijnlijkers", 10000),
        ("extreem veel waarschijnlijker", np.inf),
    ]

    def _get_probability_distribution(self, lrs: np.ndarray) -> List[Any]:
        counts = []
        lower_bound = 0
        for label, upper_bound in self.probability_ranges:
            counts.append(np.sum(np.logical_and(lower_bound <= lrs, lrs < upper_bound)))
            lower_bound = upper_bound

        return counts

    def print(self):
        print(f"results for: {self.parameters}")
        class_labels = ["colocated", "dislocated"]
        count_by_class = [np.sum(self.labels == c) for c in np.unique(self.labels)]
        print(
            f'instances: {self.labels.shape[0]} ({"; ".join([f"{v} {class_labels[c]}" for c, v in enumerate(count_by_class)])})'
        )
        if len(np.unique(self.labels)) < 2:
            cllr_min = np.nan
            cllr = np.nan
        else:
            cllr_min = lir.metrics.cllr_min(self.lrs, self.labels)
            cllr = lir.metrics.cllr(self.lrs, self.labels)
        print(f"cllr: {cllr} (min={cllr_min}; cal={cllr-cllr_min})")
        print("LR distribution:")
        distribution = [
            [name, np.sum(self.labels == label)]
            + self._get_probability_distribution(self.lrs[self.labels == label])
            for name, label in [("colocated", 1), ("dislocated", 0)]
        ]
        print(
            tabulate(
                distribution,
                headers=["", "N"] + [p[1] for p in self.probability_ranges],
            )
        )


def run_validation(
    selected_params: dict,
    data: Iterable[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]],
    estimator,
    output_dir,
):
    test_pairs = []
    test_lrs = []
    training_lrs = []
    training_labels = []
    feature_extractor = estimator.named_steps["features"]

    if output_dir is not None:
        output_dir = (
            output_dir(selected_params) if callable(output_dir) else str(output_dir)
        )
        os.makedirs(output_dir, exist_ok=True)

    for i, (batch_training_pairs, batch_test_pairs) in enumerate(data):
        # training_pairs = list(training_pairs)
        # test_pairs = list(test_pairs)
        batch_training_labels = np.asarray(
            [int(pair.is_colocated) for pair in batch_training_pairs]
        )
        if list(np.unique(batch_training_labels)) == [0, 1]:
            # both labels exist in the training data --> LRs can be calculated
            estimator.fit(batch_training_pairs, batch_training_labels)

            training_lrs.append(
                lir.to_odds(estimator.predict_proba(batch_training_pairs)[:, 1])
            )
            training_labels.append(batch_training_labels)

            lrs = lir.to_odds(estimator.predict_proba(list(batch_test_pairs))[:, 1])
            test_lrs.append(lrs)
        else:
            # no usable training data --> no LR can be calculated
            lrs = None
            test_lrs.append(np.ones(len(batch_test_pairs)))

        test_pairs.extend(batch_test_pairs)

        if output_dir is not None:
            batchdir = os.path.join(output_dir, f"batch_{i}")
            os.makedirs(batchdir, exist_ok=True)

            write_pairs_to_file(
                os.path.join(batchdir, "training_pairs.csv"), batch_training_pairs
            )
            write_pairs_to_file(
                os.path.join(batchdir, "test_pairs.csv"), batch_test_pairs
            )

            with open(os.path.join(batchdir, "training_data_stats.txt"), "wt") as f:
                for feature in feature_extractor.feature_definitions:
                    f.write(f'> {", ".join(feature.labels)}\n')
                    extr = FeatureExtractor(features=[feature])
                    values = extr.transform(batch_training_pairs)
                    for label in np.unique(batch_training_labels):
                        f.write(f"|> label {label}\n")
                        for i in range(values.shape[1]):
                            f.write(
                                f"||> count: {np.sum(batch_training_labels == label)}\n"
                            )
                            f.write(
                                f"||> average: {np.average(values[batch_training_labels == label, i])}\n"
                            )
                            f.write(
                                f"||> standard deviation: {np.std(values[batch_training_labels == label, i])}\n"
                            )
                            f.write(
                                f"||> extremes: {np.min(values[batch_training_labels == label, i])}..{np.max(values[batch_training_labels == label, i])}\n"
                            )

                    for i in range(values.shape[1]):
                        with lir.plotting.savefig(
                            os.path.join(batchdir, f"training_{label}_hist.png")
                        ) as ax:
                            for label in np.unique(batch_training_labels):
                                ax.hist(
                                    values[batch_training_labels == label, i],
                                    alpha=0.5,
                                    density=True,
                                )

            if lrs is not None:
                with lir.plotting.savefig(
                    os.path.join(batchdir, "calibrator.png")
                ) as ax:
                    ax.calibrator_fit(
                        estimator.named_steps["clf"].calibrator, score_range=(0, 1)
                    )
        # break # TODO: remove this

    training_lrs = np.concatenate(training_lrs)
    training_labels = np.concatenate(training_labels)

    test_lrs = np.concatenate(test_lrs)
    test_labels = np.asarray([int(pair.is_colocated) for pair in test_pairs])

    with lir.plotting.savefig(os.path.join(output_dir, "train_pav.png")) as ax:
        ax.pav(training_lrs, training_labels)
    with lir.plotting.savefig(os.path.join(output_dir, "train_hist.png")) as ax:
        ax.lr_histogram(training_lrs, training_labels)

    return ValidationResults(
        parameters=selected_params,
        pairs=test_pairs,
        lrs=test_lrs,
        labels=test_labels,
        feature_labels=feature_extractor.raw_labels,
    )


class CalibratedEstimator:
    def __init__(self, estimator, calibrator):
        self.estimator = estimator
        self.calibrator = calibrator

    def fit(self, X, y):
        self.estimator.fit(X, y)
        self.calibrator.fit(self.estimator.predict_proba(X)[:, 1], y)
        return self

    def predict_proba(self, X):
        p1 = self.estimator.predict_proba(X)[:, 1]
        p1 = lir.util.to_probability(self.calibrator.transform(p1))
        p0 = 1 - p1
        p_out = np.stack([p0, p1], axis=1)
        assert X.shape[0] == p_out.shape[0]
        assert p_out.shape[1] == 2
        return p_out


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    script_helper.setup_logging("colocation.log", verbosity_offset=0)


@cli.command(
    help="combines subsequent measurements from different sensors into pairs of measurements"
)
@click.option(
    "--measurements-file",
    metavar="PATH",
    required=True,
    type=click.Path(exists=True),
    help="read measurements from CSV",
)
@click.option(
    "--write-pairs",
    metavar="PATH",
    type=click.Path(),
    help="write measurements to file",
)
def pair_measurements(measurements_file: str, write_pairs: Optional[str]):
    measurements = cellsite.serialization.read_measurements_from_csv(measurements_file)
    sequential_pairs = MeasurementPairSet.from_sequential_measurements(
        measurements, within_track=False
    ).sort_by(PAIRS_SORT_KEY)
    if write_pairs is None:
        write_pairs = sys.stdout
        write_format = "csv"
    else:
        write_format = "guess"
    write_pairs_to_file(write_pairs, sequential_pairs, write_format)


@cli.command(help="modify a cell measurement pair set by applying a filter")
@click.option(
    "--pairs-file",
    metavar="PATH",
    required=True,
    type=click.Path(exists=True),
    help="read pairs from file",
)
@click.option(
    "--write-pairs",
    metavar="PATH",
    type=click.Path(),
    required=True,
    help="write pairs with features to file",
)
@click.option(
    "--max-delay",
    metavar="SECONDS",
    help="select pairs with SECONDS or less between the measurements",
    default=None,
)
@click.option(
    "--select-by-day",
    is_flag=True,
    default=False,
    help="select at most one pair per day; requires --background-measurements",
)
@click.option(
    "--background-measurements",
    metavar="PATH",
    type=click.Path(exists=True),
    required=False,
    help="read background measurements from file (used with --select-by-day)",
)
@click.option(
    "--extract-features",
    is_flag=True,
    default=False,
    help="calculates features for measurement pairs",
)
@click.option(
    "--clear-geo-data",
    is_flag=True,
    default=False,
    help="delete all geolocation data before writing pairs",
)
def filter_pairs(
    pairs_file: str,
    write_pairs: str,
    max_delay: Optional[int],
    select_by_day: bool,
    background_measurements: Optional[str],
    extract_features: bool,
    clear_geo_data: bool,
):
    with open_pairs_file(pairs_file) as pairs:
        if max_delay is not None:
            # filter pairs by delay limit
            print(f"applying max-delay {max_delay}")
            pairs = pairs.select_by_delay(0, max_delay)

        if select_by_day:
            # select the "best" pair for each interval, where "best" is defined by rarity of the right measurement
            print(f"applying selection by day")
            intervals = triage.extract_intervals(
                [pair.left.timestamp for pair in pairs],
                start_timestamp=datetime.datetime.fromisoformat(
                    "2000-01-01 05:00+00:00"
                ),
                duration=datetime.timedelta(hours=24),
            )
            assert (
                background_measurements is not None
            ), "background measurements are required in order to select the rarest cell"
            background = cellsite.serialization.read_measurements_from_csv(
                background_measurements
            )
            pairs = MeasurementPairSet.from_pairs(
                triage.select_pair_from_interval_and_sensors_by_right_cell_rarity(
                    pairs=pairs,
                    intervals=intervals,
                    background_measurements=background,
                    progress_bar=partial(tqdm, desc="selecting pairs by interval"),
                )
            )

        if extract_features:
            print(f"extracting features")
            extractor = FeatureExtractor(
                [
                    features.CalculateDistance,
                    features.CalculateAngle,
                    features.CalculateDelay,
                ],
                store_raw_features=True,
                random_factor=0,
            )

            extractor.transform(pairs)
            pairs = extractor.enriched_pairs

        if clear_geo_data:
            blacklist_types = {"geo", "cell"} if clear_geo_data else {}
        else:
            blacklist_types = {}

        print(
            f"writing {len(pairs)} cell measurement pairs ({len(pairs.select_by_colocation(True))} colocated; {len(pairs.select_by_colocation(False))} dislocated)"
        )
        write_pairs_to_file(write_pairs, pairs, blacklist_types=blacklist_types)


def evaluate(data, results_filename: str, pav_filename, lr_histogram_filename):
    calibrator = lir.ELUBbounder(lir.KDECalibrator(bandwidth=1.0))
    estimator = Pipeline(
        [
            ("features", FeatureExtractor([features.CalculateDistance])),
            ("scaler", sklearn.preprocessing.StandardScaler()),
            ("clf", CalibratedEstimator(LogisticRegression(), calibrator)),
        ]
    )

    def output_dir(selected_params: dict):
        if len(selected_params) == 0:
            return os.path.join("output", "default")
        else:
            return os.path.join(
                "output",
                f'params_{"_".join(f"{k}={v}" for k, v in selected_params.items())}',
            )

    exp = evaluation.Setup(run_validation)
    exp.parameter("data", data)
    exp.parameter("estimator", estimator)
    exp.parameter("output_dir", output_dir)
    results = exp.runDefaults()
    print("----------< RESULTS >----------")
    results.print()
    if results_filename is not None:
        results.write_results(results_filename)
    if pav_filename is not None:
        results.write_pav(pav_filename)
    if lr_histogram_filename is not None:
        results.write_lr_histogram(lr_histogram_filename)
    print("-------------------------------")


@cli.command(help="evaluates measurement pairs with a background dataset")
@click.option(
    "--training-pairs",
    metavar="PATH",
    type=click.Path(exists=True),
    required=True,
    help="use a pairs file for training",
)
@click.option(
    "--test-pairs",
    metavar="PATH",
    type=click.Path(exists=True),
    required=False,
    help="use a CSV file for testing",
)
@click.option(
    "--background-measurements",
    metavar="PATH",
    type=click.Path(exists=True),
    required=False,
    help="read background measurements from file",
)
@click.option(
    "--write-pairs",
    metavar="PATH",
    type=click.Path(exists=False),
    help="write features to a file",
)
@click.option(
    "--plot-pav",
    metavar="PATH",
    type=click.Path(exists=False),
    help="output PAV to file",
)
@click.option(
    "--plot-lr-histogram",
    metavar="PATH",
    type=click.Path(exists=False),
    help="output LR histogram to file",
)
def evaluate_with_background(
    training_pairs: str,
    test_pairs: str,
    background_measurements: str,
    write_pairs: str,
    plot_pav: str,
    plot_lr_histogram: str,
):
    with open_pairs_file(test_pairs) as test_data:
        print(
            f"found {len(test_data)} cell measurement pairs ({len(test_data.select_by_colocation(True))} colocated; {len(test_data.select_by_colocation(False))} dislocated)"
        )

        background = cellsite.serialization.read_measurements_from_csv(
            background_measurements
        )

        with open_pairs_file(training_pairs) as training_data:
            data = DataWithBackgroundCells(
                training_data.select_by_colocation(True),
                test_data,
                background,
                min_background_delay_secs=3600 * 24,
                progress_bar=tqdm,
            )
            evaluate(data, write_pairs, plot_pav, plot_lr_histogram)


if __name__ == "__main__":
    warnings.filterwarnings("error")
    cli()
