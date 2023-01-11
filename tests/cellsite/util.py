import datetime

import pytz

from cellsite import CellMeasurement, CellIdentity


def parse_time(s):
    return datetime.datetime.fromisoformat(s).replace(tzinfo=pytz.utc)


def parse_measurements(measurements):
    return [
        CellMeasurement(
            id=i,
            timestamp=parse_time(m[0]),
            track=m[1],
            device=m[2],
            cell=CellIdentity.create("GSM", 1, 1, lac=1, ci=m[3]),
        )
        for i, m in enumerate(measurements)
    ]
