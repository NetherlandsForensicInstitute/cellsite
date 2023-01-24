import datetime
import math
from typing import Iterator, Tuple, Sequence, Optional, Callable, Iterable

import numpy as np
import sklearn
from tqdm import tqdm

from cellsite import CellMeasurementSet, CellMeasurement
from colocation import MeasurementPairSet, CellMeasurementPair


class TrainTestData(
    Iterable[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]]
):
    """
    Plain train/test split, i.e. yields a single training a test set.
    """

    def __init__(self, data: MeasurementPairSet):
        self.data = data

    def __iter__(
        self,
    ) -> Iterator[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]]:
        yield sklearn.model_selection.train_test_split(list(self.data))


class DataWithBackgroundCells(
    Iterable[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]]
):
    def __init__(
        self,
        colocated_training_pairs: MeasurementPairSet,
        test_pairs: Iterable[CellMeasurementPair],
        background_measurements: CellMeasurementSet,
        min_background_delay_secs: int,
        progress_bar: Optional[Callable] = None,
    ):
        """
        Constructs a training set from background measurements for each individual test instance.

        The test set always contains one instance from `test_pairs` at a time.

        The training set for a test instance consists of colocated pairs and dislocated pairs. The colocated pairs are
        the subset of `colocated_training_pairs` with a delay similar to the test instance. The dislocated pairs are
        constructed from the test instance and the background measurements by pairing the left cell of the test instance
        with each of `background_measurements`, with the timestamp of the background measurement replaced to match the
        delay in the test instance.

        @param colocated_training_pairs: a set of colocated measurement pairs
        @param test_pairs: a set of cell measurement pairs
        @param background_measurements: a set of cell measurements
        @param min_background_delay_secs: the minimum delay between the left test measurement and the background measurement
        @param progress_bar: an optional progress bar
        """
        self.min_background_delay_secs = min_background_delay_secs
        self.colocated_training_pairs = colocated_training_pairs.with_value(
            is_colocated=True
        )
        assert (
            len(self.colocated_training_pairs) > 0
        ), "at least one colocated training pair is required"
        self.test_pairs = test_pairs
        self.background_measurements = background_measurements
        self._progress_bar = (
            progress_bar if progress_bar is not None else ProgressBar(disable=True)
        )

    def _get_random_delay(self) -> datetime.timedelta:
        return next(
            iter(self.colocated_training_pairs.sort_by("random()").limit(1))
        ).delay

    def __iter__(
        self,
    ) -> Iterator[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]]:
        assert (
            len(self.colocated_training_pairs) > 0
        ), "at least one colocated training pair is required"

        def create_background_pairs(ref, min_real_delay):
            for other in self.background_measurements.select_by_track(
                ref.track
            ).select_by_sensor(ref.sensor):
                if (
                    abs((ref.timestamp - other.timestamp).total_seconds())
                    >= min_real_delay
                ):
                    yield CellMeasurementPair(
                        ref,
                        other.with_value(
                            timestamp=ref.timestamp + self._get_random_delay()
                        ),
                        is_colocated=False,
                    )

        for test_pair in self._progress_bar(
            self.test_pairs, desc="calculating LRs", unit="pair"
        ):
            dislocated_training_pairs = list(
                create_background_pairs(test_pair.left, self.min_background_delay_secs)
            )

            training_pairs = self.colocated_training_pairs + dislocated_training_pairs
            yield training_pairs, [test_pair]


class DataBinnedByDelay(
    Iterable[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]]
):
    def __init__(
        self, training_data: MeasurementPairSet, test_pairs: MeasurementPairSet, n_bins
    ):
        self.data = training_data
        self.test_pairs = test_pairs
        self.n_bins = n_bins

    def __iter__(
        self,
    ) -> Iterator[Tuple[Sequence[CellMeasurementPair], Sequence[CellMeasurementPair]]]:
        max_delay = max(pair.delay.total_seconds() for pair in self.test_pairs) + 1
        bins = []
        for upper_bound in np.exp(
            np.arange(1, self.n_bins + 1) * math.log(max_delay) / self.n_bins
        ):
            bins.append((bins[-1][1] if len(bins) > 0 else 0, upper_bound))

        for lower_bound, upper_bound in bins:
            training_pairs = self.data.select_by_delay(lower_bound, upper_bound)
            assert (
                len(training_pairs) > 0
            ), f"no training data for {lower_bound} <= delay < {upper_bound}"
            test_pairs = self.test_pairs.select_by_delay(lower_bound, upper_bound)
            assert (
                len(test_pairs) > 0
            ), f"no test data for {lower_bound} <= delay < {upper_bound}"
            yield training_pairs, test_pairs


class ProgressBar:
    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        return tqdm(*args, **kwargs, **self._kwargs)
