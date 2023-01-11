from typing import Tuple, List, Iterable

from colocation import CellMeasurementPair


def pair2id(pair: CellMeasurementPair) -> Tuple[int, int]:
    return pair.left.id, pair.right.id


def pairs2id(pairs: Iterable[CellMeasurementPair]) -> List[Tuple[int, int]]:
    return [pair2id(pair) for pair in pairs]
