import itertools
import random
from typing import Iterable, List, Callable, Optional, Sequence, Collection

import numpy as np
import sklearn

from .features import Feature
from .measurement_pair import CellMeasurementPair, MeasurementPairSet


class FeatureExtractor(sklearn.base.TransformerMixin):
    def __init__(
        self,
        features: List[Feature],
        store_raw_features: Optional[bool] = False,
        progress_bar: Optional[Callable] = None,
        random_factor: Optional[float] = 0,
    ):
        """
        Initializes a feature extractor.

        It takes as argument a list of feature definitions, which define how features are created.

        This class can optionally store raw features, in which case a new measurement pair set is created during feature
        extraction. The new pairs have raw features as attributes. Raw means that the features are intended for later
        use or inspection, as opposed to model input, as returned by `transform`. For example, the
        `distance` feature would output a distance in km in raw mode, while `transform` returns the square root of this
        value because it makes more sense as input to linear models. Raw features can also be used as a starting point
        instead of the cell data. The pair set enriched with raw feature values can be accessed from the
        `enriched_pairs` attribute after a call to `transform`.

        @param features: the feature definitions, list of `Feature` objects.
        @param store_raw_features: if `True`, create a new measurement pair set in `enriched_pairs`.
        @param progress_bar: show a progress bar
        @param random_factor: add random noise to all feature values (value between 0..1)
        """
        self.feature_definitions = features
        self.store_raw_features = store_raw_features
        self.enriched_pairs = None
        self._progress_bar = progress_bar if progress_bar is not None else lambda x: x
        self.random_factor = random_factor

    def fit(self, X, y):
        return self

    @property
    def raw_labels(self):
        return list(itertools.chain(*(f.labels for f in self.feature_definitions)))

    def add_random(self, values: Sequence[float]) -> Sequence[float]:
        if self.add_random == 0:
            return values
        else:
            return [
                v * (1 + random.uniform(-1, 1) * self.random_factor) for v in values
            ]

    def _create_vector_from_precalculated_features(
        self, features: dict
    ) -> Iterable[float]:
        return itertools.chain(
            *(
                f.vectorize([features[label] for label in f.labels])
                for f in self.feature_definitions
            )
        )

    def _create_vector_from_cell(self, pair: CellMeasurementPair) -> Iterable[float]:
        if self.store_raw_features:
            features = dict(
                zip(
                    self.raw_labels,
                    itertools.chain(
                        *(
                            self.add_random(f.get_values(pair))
                            for f in self.feature_definitions
                        )
                    ),
                )
            )
            pair = CellMeasurementPair(**pair.as_dict(), features=features)
            self.enriched_pairs.add(pair)
            return self._create_vector_from_precalculated_features(features)
        else:
            return itertools.chain(
                *(
                    f.vectorize(self.add_random(f.get_values(pair)))
                    for f in self.feature_definitions
                )
            )

    def transform(self, pairs: Sequence[CellMeasurementPair]):
        if self.store_raw_features:
            self.enriched_pairs = MeasurementPairSet.create()

        rows = []
        for pair in self._progress_bar(pairs):
            if (
                hasattr(pair, "features")
                and next(iter(pair.features.values())) is not None
            ):
                rows.append(
                    list(self._create_vector_from_precalculated_features(pair.features))
                )
            else:
                rows.append(list(self._create_vector_from_cell(pair)))

        features = np.stack(rows, axis=0)
        assert features.shape[0] == len(
            pairs
        ), f"expected feature vector length: {len(pairs)}; found: {features.shape[0]}"
        return features
