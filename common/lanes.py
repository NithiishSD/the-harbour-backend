"""Sea lanes: a graph of open-water waypoints along the routes ships use, and routing over it.

The lanes are a backbone of the world's main routes: the Asia-Pacific rim and the North Pacific (the
Singapore -> Los Angeles route is the long spine), the American west coast, the Indian Ocean, Red Sea and
Suez, the Persian Gulf, the Mediterranean, Atlantic Europe, the North and South Atlantic, the American
east coast and Caribbean, Panama, Africa, Australia and New Zealand. Every port in geo/ports.csv and
geo/world_ports.csv joins the graph at one node (`Port.node`).

A route from anywhere is: the position, the cheapest chain of lane nodes to the destination's node, then the
port. Between two nodes a ship goes straight, so the nodes are placed where a straight line stays in water.
Longitudes run on past 180 (195 is 165 W); `norm_lon` turns them back for output.

The same graph gives the live AIS subscription boxes (`corridor_boxes`), the lanes drawn on the map and the
weather points (`weather_points`).
"""

import heapq
import math
from collections.abc import Iterable
from functools import cache

from common.geo import Port, haversine_km

KM_PER_NM = 1.852

# ponytail: hand-placed nodes, checked by eye on the satellite map, not a coastline-aware router. Add a node
# wherever a leg cuts a corner of land (the straight leg is drawn, so a mistake is visible).
NODES: dict[str, tuple[float, float]] = {
    # Singapore, Malacca Strait, Andaman Sea
    "SIN_W": (1.17, 103.55), "SIN_E": (1.30, 104.50),
    "MAL_1": (1.90, 102.30), "MAL_2": (2.60, 101.20), "MAL_3": (3.00, 100.50), "MAL_4": (4.20, 99.30),
    "PEN": (5.60, 99.20), "AND0": (6.00, 97.20), "AND": (6.30, 94.80),
    # Gulf of Thailand, Java Sea
    "GOT1": (5.00, 105.20), "GOT2": (8.30, 103.20), "GOT": (12.30, 100.90),
    "GAS": (-3.00, 107.00), "JKT": (-5.80, 106.90), "JAV": (-5.50, 111.00), "SUB": (-6.70, 112.80),
    # South China Sea, Vietnam, Taiwan Strait
    "SCS_S": (6.00, 108.50), "SCS_C": (12.00, 113.50), "SCS_N": (19.00, 119.00), "LUZ": (20.80, 121.20),
    "VT": (9.60, 107.80), "VN1": (16.00, 109.50), "VN2": (18.20, 107.60), "HPH": (20.40, 107.20),
    "HK": (21.80, 114.40), "MNL": (14.30, 120.20), "KHH": (22.30, 120.00),
    "TWS": (24.20, 119.20), "TWS_N": (26.40, 120.90), "TPE": (25.70, 122.00), "TWE": (23.00, 122.40),
    # East China Sea, Yellow Sea, Korea
    "SHA": (30.80, 123.20), "ECS": (29.50, 125.50), "TOK": (28.90, 129.10), "JEJ": (32.40, 127.30),
    "BSN": (34.30, 128.90), "YSE": (35.50, 124.50), "ICN": (37.00, 125.50), "QDO": (35.60, 121.00),
    "SDE": (37.30, 123.20), "BOH": (38.40, 121.50),
    # Japan and the Pacific side of the Philippines
    "JSE": (38.00, 132.00), "TSG": (41.60, 140.50), "TSE": (41.70, 142.50),
    "PHS": (23.00, 126.00), "JPS": (28.00, 134.00), "OSA": (33.80, 135.00), "NGO": (34.00, 137.00),
    "TYO": (34.80, 139.70), "JPE": (33.00, 142.00),
    # North Pacific (the Singapore -> Los Angeles spine) and Hawaii
    "NP1": (40.00, 160.00), "NP2": (44.00, 180.00), "NP3": (45.00, 195.00), "NP4": (42.00, 215.00),
    "NP5": (37.00, 232.00), "HI2": (28.00, 185.00), "HNL": (21.00, 201.80), "HI1": (30.00, 225.00),
    # American west coast
    "LA": (33.55, 241.55), "SD": (32.50, 242.50), "PTC": (34.10, 239.00), "MRY": (36.40, 237.50),
    "SF": (37.70, 237.20), "COL": (46.20, 235.60), "JDF": (48.50, 235.20), "JDF2": (48.25, 236.80),
    "PRR": (54.10, 228.50),
    # Mexico, Central America, Panama
    "BAJ": (27.00, 244.40), "CAB": (22.30, 250.00), "MZO": (18.80, 254.80), "LZC": (17.50, 257.50),
    "TEH": (15.00, 264.00), "GUA": (12.30, 269.50), "NIC": (9.30, 273.50), "PAN": (6.80, 279.50),
    "BLB": (8.30, 280.50),
    # Bay of Bengal, India, Sri Lanka
    "IO1": (5.80, 88.00), "SLS": (5.50, 80.50), "SLE": (6.80, 82.80), "CMB": (6.50, 79.60),
    "KOC": (9.70, 75.70), "GOA": (14.00, 72.50), "MUM": (18.70, 72.40), "CHE": (13.00, 81.00),
    # Arabian Sea, Gulf of Aden, Red Sea, Suez, Persian Gulf
    "ARBS": (8.00, 70.00), "ARBW": (11.00, 56.00), "GAD": (12.50, 50.00), "ADN": (12.00, 45.50),
    "BAB": (12.60, 43.30), "RS1": (16.00, 41.50), "RS2": (21.00, 38.00), "RS3": (26.50, 34.80),
    "SUZ": (29.60, 32.60), "PSD": (31.90, 32.40), "SLL": (16.50, 54.00), "OMN": (23.80, 59.50),
    "HRM": (26.50, 56.50), "PGU": (26.80, 53.00), "JBA": (25.40, 54.60),
    # Mediterranean and Gibraltar
    "MED1": (33.80, 27.00), "MED2": (35.00, 15.00), "SIC": (37.40, 11.60), "MED3": (38.00, 5.00),
    "ALB": (36.20, -2.00), "GIB": (36.00, -5.70), "VLC": (39.20, 0.20), "BCN": (41.20, 2.60),
    "MRS": (43.00, 5.50), "GEN": (43.80, 8.50),
    # Atlantic Europe
    "SV": (36.80, -9.50), "LIS": (38.50, -9.60), "FIN": (43.00, -10.00), "USH": (48.60, -6.00),
    "CHN": (50.00, -2.50), "CH2": (50.20, 0.00), "LEH": (49.60, -0.50), "DOV": (51.00, 1.50),
    "FXT": (51.90, 2.10), "RTM": (52.00, 3.50), "NS1": (54.00, 3.50), "HAM": (54.00, 7.80),
    # North Atlantic, US east coast, Gulf of Mexico, Caribbean
    "AZO": (38.00, -28.00), "ATL1": (47.00, -30.00), "ATL2": (42.00, -58.00), "NYC": (40.30, -73.50),
    "ORF": (36.90, -75.60), "HAT": (34.50, -75.30), "SAVN": (31.90, -80.50), "MIA": (25.70, -79.90),
    "FLS": (24.00, -81.50), "GOM": (26.00, -88.00), "HOU": (29.20, -94.50), "NOL": (28.90, -89.20),
    "WIN": (20.00, -73.80), "CAR": (14.00, -76.50), "CLN": (9.70, -79.90),
    # South Atlantic, Africa
    "ATS0": (31.00, -17.00), "ATS1": (20.00, -25.00), "ATS2": (0.00, -30.00), "ATS3": (-25.00, -38.00),
    "SNT": (-24.50, -45.50), "AFW3": (3.00, -12.00), "AFW1": (-30.00, 12.00), "CGH": (-35.50, 19.50),
    "CPT": (-34.20, 18.00), "SAF": (-35.30, 27.00), "DUR": (-30.10, 31.50),
    # Indian Ocean south, Australia, New Zealand
    "IOA": (-8.00, 72.00), "IOC": (-22.00, 66.00), "IOD": (-30.00, 56.00), "IOB": (-30.00, 42.00),
    "SUN": (-6.00, 105.80), "IOE": (-11.00, 106.00), "AUS0": (-28.00, 112.50), "FRE": (-32.10, 115.20),
    "AUS1": (-35.50, 114.50), "AUS2": (-36.50, 125.00), "AUS3": (-39.30, 141.00), "MEL": (-38.60, 144.60),
    "AUS4": (-39.80, 147.50), "AUS5": (-37.50, 150.80), "SYD": (-33.90, 151.50), "AUS6": (-28.00, 154.30),
    "BNE": (-27.40, 153.80), "AKL": (-36.50, 175.20),
}  # fmt: skip

EDGES: tuple[tuple[str, str], ...] = (
    ("SIN_W", "SIN_E"), ("SIN_W", "MAL_1"), ("MAL_1", "MAL_2"), ("MAL_2", "MAL_3"), ("MAL_3", "MAL_4"),
    ("MAL_4", "PEN"), ("PEN", "AND0"), ("MAL_4", "AND0"), ("AND0", "AND"),
    ("SIN_E", "GOT1"), ("GOT1", "GOT2"), ("GOT2", "GOT"), ("GOT1", "SCS_S"), ("SIN_E", "SCS_S"),
    ("SIN_E", "GAS"), ("GAS", "JKT"), ("GAS", "JAV"), ("JKT", "JAV"), ("JAV", "SUB"),
    ("SCS_S", "VT"), ("SCS_S", "SCS_C"), ("SCS_C", "SCS_N"), ("SCS_C", "VN1"), ("VT", "VN1"),
    ("VN1", "VN2"), ("VN2", "HPH"),
    ("SCS_N", "LUZ"), ("SCS_N", "HK"), ("SCS_N", "MNL"), ("SCS_N", "KHH"), ("SCS_N", "TWS"),
    ("HK", "TWS"), ("KHH", "TWS"), ("TWS", "TWS_N"), ("TWS_N", "SHA"), ("TWS_N", "TPE"),
    ("LUZ", "TWE"), ("TWE", "TPE"), ("TWE", "PHS"), ("LUZ", "PHS"),
    ("TPE", "ECS"), ("SHA", "ECS"), ("ECS", "TOK"), ("ECS", "JEJ"), ("SHA", "JEJ"), ("JEJ", "BSN"),
    ("JEJ", "YSE"), ("YSE", "ICN"), ("YSE", "QDO"), ("QDO", "SDE"), ("SDE", "BOH"),
    ("TOK", "JPS"), ("PHS", "JPS"), ("JPS", "OSA"), ("JPS", "NGO"), ("JPS", "JPE"), ("NGO", "TYO"),
    ("JPE", "TYO"), ("BSN", "JSE"), ("JSE", "TSG"), ("TSG", "TSE"), ("TSE", "NP1"), ("TSE", "NP2"),
    ("JPE", "NP1"), ("NP1", "NP2"), ("NP2", "NP3"), ("NP3", "NP4"), ("NP4", "NP5"), ("NP5", "LA"),
    ("NP5", "SF"), ("NP4", "JDF"), ("NP3", "PRR"),
    ("LA", "SD"), ("LA", "PTC"), ("PTC", "MRY"), ("MRY", "SF"), ("SF", "COL"), ("COL", "JDF"),
    ("JDF", "JDF2"), ("PRR", "JDF"),
    ("LA", "HI1"), ("HI1", "HNL"), ("JPE", "HI2"), ("HI2", "HNL"),
    ("LA", "BAJ"), ("BAJ", "CAB"), ("CAB", "MZO"), ("MZO", "LZC"), ("LZC", "TEH"), ("TEH", "GUA"),
    ("GUA", "NIC"), ("NIC", "PAN"), ("PAN", "BLB"),
    # Bay of Bengal, India, Sri Lanka
    ("AND", "IO1"), ("IO1", "SLS"), ("SLS", "CMB"), ("SLS", "SLE"), ("SLE", "CHE"), ("CMB", "KOC"),
    ("KOC", "GOA"), ("GOA", "MUM"), ("CMB", "ARBS"), ("KOC", "ARBS"), ("ARBS", "ARBW"), ("MUM", "ARBW"),
    # Gulf of Aden, Red Sea, Suez, Persian Gulf
    ("ARBW", "GAD"), ("GAD", "ADN"), ("ADN", "BAB"), ("BAB", "RS1"), ("RS1", "RS2"), ("RS2", "RS3"),
    ("RS3", "SUZ"), ("SUZ", "PSD"), ("ARBW", "SLL"), ("ARBW", "OMN"), ("OMN", "HRM"), ("HRM", "PGU"),
    ("PGU", "JBA"),
    # Mediterranean and Gibraltar
    ("PSD", "MED1"), ("MED1", "MED2"), ("MED2", "SIC"), ("SIC", "MED3"), ("MED3", "ALB"), ("ALB", "GIB"),
    ("MED3", "VLC"), ("VLC", "BCN"), ("BCN", "MRS"), ("MRS", "GEN"),
    # Atlantic Europe
    ("GIB", "SV"), ("SV", "LIS"), ("LIS", "FIN"), ("FIN", "USH"), ("USH", "CHN"), ("CHN", "CH2"),
    ("CH2", "DOV"), ("CHN", "LEH"), ("DOV", "FXT"), ("DOV", "RTM"), ("RTM", "NS1"), ("NS1", "HAM"),
    ("FXT", "RTM"),
    # North Atlantic, US east coast, Gulf of Mexico, Caribbean, Panama
    ("GIB", "AZO"), ("AZO", "ATL2"), ("USH", "ATL1"), ("ATL1", "ATL2"), ("ATL2", "NYC"), ("NYC", "ORF"),
    ("ORF", "HAT"), ("HAT", "SAVN"), ("SAVN", "MIA"), ("MIA", "FLS"), ("FLS", "GOM"), ("GOM", "HOU"),
    ("GOM", "NOL"), ("MIA", "WIN"), ("WIN", "CAR"), ("CAR", "CLN"), ("CLN", "BLB"),
    # South Atlantic and Africa
    ("GIB", "ATS0"), ("ATS0", "ATS1"), ("ATS1", "ATS2"), ("ATS2", "ATS3"), ("ATS3", "SNT"),
    ("AFW3", "ATS1"), ("AFW3", "ATS2"), ("AFW1", "AFW3"), ("CGH", "AFW1"), ("CGH", "CPT"), ("CGH", "SAF"),
    ("SAF", "DUR"), ("SLS", "IOA"), ("IOA", "IOC"), ("IOC", "IOD"), ("IOD", "IOB"), ("IOB", "DUR"),
    # Australia and New Zealand
    ("JKT", "SUN"), ("SUN", "IOE"), ("IOE", "AUS0"), ("AUS0", "FRE"), ("FRE", "IOB"), ("FRE", "AUS1"),
    ("AUS1", "AUS2"), ("AUS2", "AUS3"), ("AUS3", "MEL"), ("MEL", "AUS4"), ("AUS4", "AUS5"),
    ("AUS5", "SYD"), ("SYD", "AUS6"), ("AUS6", "BNE"), ("SYD", "AKL"),
)  # fmt: skip

NEAREST_NODES = 4  # the start of a route is chosen among this many nearest nodes
MAX_TO_LANE_KM = 3000  # further from every lane than this: not a ship this network knows


def norm_lon(lon: float) -> float:
    return lon - 360 if lon > 180 else lon


def _km(a: tuple[float, float], b: tuple[float, float]) -> float:
    return haversine_km(a[0], a[1], b[0], b[1])


@cache
def _adjacent() -> dict[str, list[tuple[str, float]]]:
    adj: dict[str, list[tuple[str, float]]] = {n: [] for n in NODES}
    for a, b in EDGES:
        d = _km(NODES[a], NODES[b])
        adj[a].append((b, d))
        adj[b].append((a, d))
    return adj


@cache
def _from_goal(goal: str) -> tuple[dict[str, float], dict[str, str]]:
    """Dijkstra from the goal: the km from every node to it, and each node's next step towards it."""
    dist = {goal: 0.0}
    step: dict[str, str] = {}
    queue = [(0.0, goal)]
    while queue:
        d, node = heapq.heappop(queue)
        if d > dist.get(node, math.inf):
            continue
        for other, km in _adjacent()[node]:
            if d + km < dist.get(other, math.inf):
                dist[other] = d + km
                step[other] = node  # from `other` the way to the goal goes through `node`
                heapq.heappush(queue, (d + km, other))
    return dist, step


def route(lat: float, lon: float, port: Port) -> list[list[float]] | None:
    """[position, lane nodes..., port] from a position to a port; None when the ship is far from every lane
    or the port is not on the lanes."""
    if port.node not in NODES:
        return None
    here = (lat, lon + 360 if lon < -30 else lon)  # the lanes' longitudes run past 180
    dist, step = _from_goal(port.node)
    nearest = sorted(NODES, key=lambda n: _km(here, NODES[n]))[:NEAREST_NODES]
    nearest = [n for n in nearest if n in dist]
    if not nearest or _km(here, NODES[nearest[0]]) > MAX_TO_LANE_KM:
        return None
    start = min(nearest, key=lambda n: _km(here, NODES[n]) + dist[n])
    chain = [start]
    while chain[-1] != port.node:
        chain.append(step[chain[-1]])
    nodes = [[NODES[n][0], round(norm_lon(NODES[n][1]), 5)] for n in chain]
    return [[lat, lon], *nodes, [port.lat, port.lon]]


def length_km(path: list[list[float]]) -> float:
    return sum(haversine_km(a[0], a[1], b[0], b[1]) for a, b in zip(path, path[1:], strict=False))


def lanes(edges: Iterable[tuple[str, str]] | None = None) -> list[list[list[float]]]:
    """The lanes (all by default) as [[lat, lon], [lat, lon]], longitudes in -180..180, to draw on the map."""
    return [
        [[NODES[a][0], round(norm_lon(NODES[a][1]), 5)], [NODES[b][0], round(norm_lon(NODES[b][1]), 5)]]
        for a, b in (EDGES if edges is None else edges)
    ]


def weather_points() -> list[tuple[str, float, float]]:
    """(point id, lat, lon) of the weather points: every lane node, so wind and waves are known on lanes."""
    return [(f"LANE-{n}", lat, round(norm_lon(lon), 2)) for n, (lat, lon) in NODES.items()]


def _samples(
    a: tuple[float, float], b: tuple[float, float], step_km: float = 60.0
) -> Iterable[tuple[float, float]]:
    n = max(1, math.ceil(_km(a, b) / step_km))
    for i in range(n + 1):
        f = i / n
        yield a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f


def chain(start: str, goal: str) -> list[str]:
    """The lane nodes from `start` to `goal`, both included."""
    _, step = _from_goal(goal)
    nodes = [start]
    while nodes[-1] != goal:
        nodes.append(step[nodes[-1]])
    return nodes


def corridor_boxes(
    cell: float = 5.0,
    pad: int = 1,
    edges: Iterable[tuple[str, str]] | None = None,
    around: Iterable[tuple[float, float, float]] = (),
) -> tuple[tuple[float, float, float, float], ...]:
    """(min_lat, min_lon, max_lat, max_lon) boxes for the live AIS subscription: every `cell`-degree
    square the given lanes (all of them by default) cross, widened by `pad` squares, plus a square block
    round each (lat, lon, km) in `around`; merged along rows. Boxes across the 180th meridian are split."""
    squares: set[tuple[int, int]] = set()
    for a, b in EDGES if edges is None else edges:
        for lat, lon in _samples(NODES[a], NODES[b]):
            r, c = math.floor(lat / cell), math.floor(lon / cell)
            squares |= {(r + dr, c + dc) for dr in range(-pad, pad + 1) for dc in range(-pad, pad + 1)}
    for lat, lon, km in around:
        dlat = km / 111.0
        dlon = km / (111.0 * max(0.2, math.cos(math.radians(lat))))
        lon = lon + 360 if lon < -30 else lon
        for r in range(math.floor((lat - dlat) / cell), math.floor((lat + dlat) / cell) + 1):
            for c in range(math.floor((lon - dlon) / cell), math.floor((lon + dlon) / cell) + 1):
                squares.add((r, c))
    rows: dict[int, list[int]] = {}
    for r, c in squares:
        rows.setdefault(r, []).append(c)
    boxes: list[tuple[float, float, float, float]] = []
    for r, cols in sorted(rows.items()):
        south, north = max(-90.0, r * cell), min(90.0, (r + 1) * cell)
        for west_c, east_c in _runs(sorted(cols)):
            west, east = west_c * cell, (east_c + 1) * cell
            if east <= 180:
                boxes.append((south, west, north, east))
            elif west >= 180:
                boxes.append((south, west - 360, north, east - 360))
            else:
                boxes += [(south, west, north, 180.0), (south, -180.0, north, east - 360)]
    return tuple(boxes)


def _runs(cols: list[int]) -> list[tuple[int, int]]:
    """Sorted column numbers as (first, last) runs of consecutive ones."""
    runs = [(cols[0], cols[0])]
    for c in cols[1:]:
        first, last = runs[-1]
        runs[-1:] = [(first, c)] if c == last + 1 else [(first, last), (c, c)]
    return runs
