import logging
import warnings
from typing import Tuple, Optional

from cellsite import CellIdentity


LOG = logging.getLogger(__name__)


def resolve_cell(
    cell_resolver, timestamp, radio, mcc, mnc, lac, ci
) -> Tuple[CellIdentity, Optional[dict]]:
    if radio == "GSM" or radio == "UMTS":
        ci = ci & 0xFFFF
    ci = CellIdentity.create(radio=radio, mcc=mcc, mnc=mnc, lac=lac, ci=ci, eci=ci)
    properties = cell_resolver.get(timestamp, ci)
    if properties is None:
        LOG.warning(f"unable to resolve {ci} at {timestamp}")

    return ci, properties
