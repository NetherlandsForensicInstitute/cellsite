import collections
import math
from abc import abstractmethod
from typing import Sequence

import geopy.distance

from celldb import CellDatabase
from cellsite.geography import normalize_angle, azimuth, Angle
from .measurement_pair import CellMeasurementPair


class Feature:
    """
    A feature turns a measurement pair into numeric values.

    The `labels` attribute are human-readable names which correspond to the values returned by `get_values`.
    """

    def __init__(self, labels):
        self.labels = labels

    @abstractmethod
    def get_values(self, pair: CellMeasurementPair) -> Sequence[float]:
        """
        Calculates a list of one or more values that should be reasonably interpreted.

        @param pair: the instance to take features from
        @return: a list of values
        """
        raise NotImplemented

    @abstractmethod
    def vectorize(self, vector: Sequence[float]) -> Sequence[float]:
        """
        Transforms the output of `get_values` to a list of one or more `float` values that
        are fed as input to a model. This may include a log or square root transformation or something else that makes it
        more difficult to interpret.

        @return: a list of values
        """
        raise NotImplemented


StaticFeature = collections.namedtuple("Feature", ["labels", "get_values", "vectorize"])


def calculate_distance(pair: CellMeasurementPair) -> Sequence[float]:
    coords = [cell.wgs84.wgs84().latlon for cell in (pair.left.geo, pair.right.geo)]
    distance = geopy.distance.distance(*coords).km * 1000
    return [distance]


def calculate_angle(pair: CellMeasurementPair) -> Sequence[float]:
    cell1 = pair.left.geo
    cell2 = pair.right.geo

    ref_to_query_azi = azimuth(cell1.wgs84, cell2.wgs84)
    if ref_to_query_azi.isnan():
        ref_angle = Angle(degrees=0)
        query_angle = Angle(degrees=0)
    else:
        ref_angle = normalize_angle(cell1.azimuth - ref_to_query_azi)
        query_angle = normalize_angle(
            ref_to_query_azi + Angle(radians=math.pi) - cell2.azimuth
        )

    return [ref_angle.degrees, query_angle.degrees]


CalculateDistance = StaticFeature(
    ("distance_m",),
    calculate_distance,
    lambda v: [math.sqrt(v[0])],
)

CalculateAngle = StaticFeature(
    ("cell1_angle", "cell2_angle"),
    calculate_angle,
    lambda v: [abs(v[0] - v[1]), math.sqrt(abs(v[0] * v[1]))],
)


CalculateDelay = StaticFeature(
    ("measurement_delay",),
    lambda pair: [(pair.left.timestamp - pair.right.timestamp).total_seconds()],
    lambda v: [math.log(abs(v[0]) + 1)],
)


class CountCloser:
    """
    For a cell measurement pair, counts the number of cells which are closer to the left cell than the right cell.

    Requires a cell database to operate.
    """

    labels = ("n_closer",)

    def __init__(self, celldb: CellDatabase):
        self.celldb = celldb

    def get_values(self, pair: CellMeasurementPair) -> Sequence[float]:
        distance_m = calculate_distance(pair)[0]
        n_closer = len(
            self.celldb.search(
                pair.left.celldb.wgs84, distance_m + 1, date=pair.left.timestamp
            )
        )
        return [n_closer]

    @staticmethod
    def vectorize(vector: Sequence[float]) -> Sequence[float]:
        return vector
