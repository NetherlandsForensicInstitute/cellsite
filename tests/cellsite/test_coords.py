import math

from cellsite.coord import RD_TO_WGS84, RdPoint, WgsPoint, WGS84_TO_RD


def do_compare(msg, expected, actual, tolerance=0.0001):
    for i in [0, 1]:
        if not math.isclose(expected[i], actual[i], abs_tol=tolerance):
            raise ValueError(
                "%s. expected: %s, but was: %s"
                % (
                    msg,
                    ",".join(str(x) for x in expected),
                    ",".join(str(x) for x in actual),
                )
            )


class TestCoords:
    def test_coords(self):
        do_compare(
            "rd to wsg84", (51.7545, 4.0211), RD_TO_WGS84.transform(60677, 419308)[::-1]
        )
        do_compare(
            "wsg84 to rd",
            (60677, 419308),
            WGS84_TO_RD.transform(*[51.7545, 4.0211][::-1]),
            1,
        )
        do_compare(
            "Point(rd) to wsg84",
            (51.7545, 4.0211),
            RdPoint(x=60677, y=419308).wgs84().latlon,
        )
        do_compare(
            "Point(wsg84) to rd",
            (60677, 419308),
            WgsPoint(lat=51.7545, lon=4.0211).rd().xy,
            1,
        )
