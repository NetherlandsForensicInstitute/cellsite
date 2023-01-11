import datetime
import random
from functools import lru_cache
from typing import List, Optional, Sequence, Tuple

import numpy as np

from cellresolution.antennadatabase import AntennaDatabase
from cellsite import Antenna, CellIdentity
from cellsite.coord import Point
from cellsite.geography import euclidean_distance


class AntennaDummyDatabase(AntennaDatabase):
    """
    Dummy implementation of an AntennaDatabase, with fake antennas at locations in a rectangular grid.
    """

    def __init__(
        self,
        number_of_horizontal_positions: int,
        number_of_vertical_positions: int,
        antennas_per_position: int = 1,
    ):
        self._antennas_per_position = antennas_per_position
        self._x_offset = 100000
        self._y_offset = 100000
        self._x_interval = 500
        self._y_interval = 500
        self._n_antennas = 0
        self._antennas: List[List[List[Antenna]]] = []
        eci_iterator = iter(
            range(
                number_of_horizontal_positions
                * number_of_vertical_positions
                * self._antennas_per_position
            )
        )
        for x in range(number_of_horizontal_positions):
            rdx = self._x_offset + x * self._x_interval
            self._antennas.append([])
            for y in range(number_of_vertical_positions):
                self._antennas[x].append([])
                rdy = self._y_offset + y * self._y_interval
                azi_offset = random.randint(0, 359)
                for i in range(self._antennas_per_position):
                    a = Antenna(
                        rdx=rdx,
                        rdy=rdy,
                        azimuth=(azi_offset + i * 120) % 360,
                        zipcode="1234XL",
                        city="Amsterdam",
                        ci=CellIdentity.create("LTE", eci=next(eci_iterator)),
                    )
                    self._antennas[x][y].append(a)
                    self._n_antennas += 1

    def get(self, date: datetime.date, ci: CellIdentity) -> Optional[Antenna]:
        raise NotImplementedError

    @lru_cache(maxsize=None)
    def search(
        self,
        coords: Point,
        distance_limit_m: float = None,
        distance_lower_limit_m: float = None,
        date: datetime.date = None,
        radio: Optional[Tuple[str]] = None,
        mcc: int = None,
        mnc: int = None,
        count_limit: Optional[int] = 10000,
        exclude: Optional[Antenna] = None,
    ) -> Sequence[Antenna]:
        # round the location to one of the points in the grid
        x_index = min(
            len(self._antennas) - 1,
            max(0, round((coords.rd().x() - self._x_offset) / self._x_interval)),
        )
        y_index = min(
            len(self._antennas[x_index]) - 1,
            max(0, round((coords.rd().y() - self._y_offset) / self._y_interval)),
        )

        candidates = []

        x_offsets = np.arange(len(self._antennas))
        x_choices = x_index + np.stack([x_offsets, -x_offsets], -1).reshape(-1)[1:]
        x_choices = x_choices[x_choices >= 0]
        x_choices = x_choices[x_choices < len(self._antennas)]

        sort_candidates = lambda l: [item[1] for item in sorted(l, key=lambda x: x[0])]

        for x in x_choices:
            if (
                distance_limit_m is not None
                and euclidean_distance(coords, self._antennas[x][y_index][0].coords)
                > distance_limit_m
            ):
                break

            y_offsets = np.arange(len(self._antennas[x]))
            y_choices = y_index + np.stack([y_offsets, -y_offsets], -1).reshape(-1)[1:]
            y_choices = y_choices[y_choices >= 0]
            y_choices = y_choices[y_choices < len(self._antennas[x])]
            for y in y_choices:
                for i in range(self._antennas_per_position):

                    candidate = self._antennas[x][y][i]
                    dist = euclidean_distance(coords, candidate.coords)
                    if distance_limit_m is not None and dist > distance_limit_m:
                        break

                    if candidate == exclude:
                        continue

                    candidates.append((dist, candidate))

        return sort_candidates(candidates)[:count_limit]

    def __len__(self) -> int:
        return self._n_antennas
