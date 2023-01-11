import warnings
from typing import Optional

from cellsite import CellIdentity, Properties


def exception(ci: CellIdentity, results: Properties) -> Optional[Properties]:
    raise ValueError(f"duplicate cell id {ci} (not allowed by current policy)")


def warn(ci: CellIdentity, results: Properties) -> Optional[Properties]:
    warnings.warn(f"duplicate cell id {ci}")
    return results[0]


def take_first(ci: CellIdentity, results: Properties) -> Optional[Properties]:
    return results[0]


def drop(ci: CellIdentity, results: Properties) -> Optional[Properties]:
    return None
