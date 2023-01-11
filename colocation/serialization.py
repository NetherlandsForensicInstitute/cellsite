import logging
from typing import Any, Tuple

from cellsite import CellMeasurement
from cellsite.serialization import (
    BasicDictionizerDedictionizer,
    Dictionizer,
    Dedictionizer,
    CELL_MEASUREMENT_SERIALIZERS,
    _collapse_dict,
    _expand_dict,
)
from .measurement_pair import CellMeasurementPair

LOG = logging.getLogger(__name__)


class SerializeCellMeasurement(BasicDictionizerDedictionizer):
    def __init__(self):
        super().__init__("msrmnt", CellMeasurement)

    def to_dict(self, key: str, value: CellMeasurement) -> Tuple[str, dict[str, str]]:
        key = self.add_type_indicator(key)
        return key, value.as_dict()

    def from_dict(self, key: str, value: dict[str, str]) -> Tuple[str, CellMeasurement]:
        key = self.remove_type_indicator(key)
        return key, CellMeasurement(**value)


class SerializeFeatures(Dictionizer, Dedictionizer):
    def is_dictable(self, key: str, value: Any) -> bool:
        return key == "features"

    def is_dedictable(self, key: str, value: Any) -> bool:
        return key == "features"

    def to_dict(
        self, key: str, value: dict[str, dict[str, float]]
    ) -> Tuple[str, dict[str, str]]:
        return key, {k: float(v) for k, v in value.items()}

    def from_dict(
        self, key: str, value: dict[str, str]
    ) -> Tuple[str, dict[str, float]]:
        return key, {k: str(v) for k, v in value.items()}


PAIR_SERIALIZERS = CELL_MEASUREMENT_SERIALIZERS + [
    SerializeCellMeasurement(),
    # SerializeFeatures(),
]


def serialize_cell_measurement_pair(
    pair: CellMeasurementPair, blacklist_types: set = None
) -> dict[str, Any]:
    if blacklist_types is None:
        blacklist_types = set()
    return _collapse_dict(pair.as_dict(), PAIR_SERIALIZERS, blacklist_types)


def deserialize_cell_measurement_pair(pair: dict[str, Any]) -> CellMeasurementPair:
    return CellMeasurementPair(**_expand_dict(pair, PAIR_SERIALIZERS))
