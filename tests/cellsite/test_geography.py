import math
from math import pi
import unittest

from cellsite import RdPoint, Point
from cellsite.coord import move_point
from cellsite.geography import azimuth, normalize_angle, Angle


def azimuth_rd(coord1: Point, coord2: Point) -> float:
    """
    Returns the azimuth in radians of the line from `coord1` to `coord2`.
    """
    coord1 = coord1.rd()
    coord2 = coord2.rd()
    dx = coord2.x - coord1.x
    dy = coord2.y - coord1.y
    if dy > 0:
        return math.atan(dx / dy)
    elif dy < 0:
        return math.pi + math.atan(dx / dy)
    elif dx < 0:
        return 1.5 * math.pi
    elif dx > 0:
        return 0.5 * math.pi
    else:
        return float("nan")  # coords are identical


class GeographyTest(unittest.TestCase):
    def test_azimuth(self):
        p = RdPoint(150000, 450000)
        pairs = [
            (pi / 2, (1, 0)),
            (pi, (0, -1)),
            (pi * -0.5, (-1, 0)),
            (0, (0, 1)),
        ]

        for expected, (dx, dy) in pairs:
            moved = move_point(p, east_m=dx, north_m=dy)
            self.assertAlmostEqual(
                azimuth_rd(p, moved), azimuth(p, moved).radians, places=2
            )
            self.assertAlmostEqual(expected, azimuth(p, moved).radians)

        self.assertTrue(math.isnan(azimuth(p, p).radians))

    def test_normalize_angle(self):
        for i in range(-2, 3):
            mod = i * pi * 2
            pairs = [
                (0, mod + 0),
                (pi / 2, mod + pi / 2),
                (pi, mod + pi - 1e-10),
                (-pi, mod + pi + 1e-10),
                (-pi / 2, mod + pi * 1.5),
                (0, mod + pi * 2),
                (pi / 2, mod + pi * 8.5),
            ]
            for expected, input in pairs:
                self.assertAlmostEqual(
                    Angle(radians=expected).degrees,
                    normalize_angle(Angle(radians=input)).degrees,
                )
