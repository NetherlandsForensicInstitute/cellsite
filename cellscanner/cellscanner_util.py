import logging
from typing import Tuple, Optional

from cellsite import CellIdentity


LOG = logging.getLogger(__name__)


def resolve_cell(
    cell_resolver, timestamp, radio, mcc, mnc, lac, ci
) -> Tuple[CellIdentity, Optional[dict]]:
    cell = CellIdentity.create(radio=radio, mcc=mcc, mnc=mnc, lac=lac, ci=ci, eci=ci)
    if cell_resolver is None:
        return cell, None
    else:
        properties = cell_resolver.get(timestamp, cell)
        if properties is None:
            LOG.warning(f"unable to resolve {cell} at {timestamp}")

        return cell, properties
