
from typing import Tuple


MBR = Tuple[float, float, float, float]

def from_point(x: float, y: float) -> MBR:
    return (x, x, y, y)

def rect_from_point_radius(x: float, y: float, r: float) -> MBR:
    return (x - r, x + r, y - r, y + r)

def intersects(a: MBR, b: MBR) -> bool:
    ax1, ax2, ay1, ay2 = a
    bx1, bx2, by1, by2 = b
    return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)

def area(m: MBR) -> float:
    x1, x2, y1, y2 = m
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)

def expand(a: MBR, b: MBR) -> MBR:
    ax1, ax2, ay1, ay2 = a
    bx1, bx2, by1, by2 = b
    return (min(ax1, bx1), max(ax2, bx2), min(ay1, by1), max(ay2, by2))

def enlargement(a: MBR, b: MBR) -> float:
    return area(expand(a, b)) - area(a)

def mindist_point_mbr(px: float, py: float, m: MBR) -> float:
    x1, x2, y1, y2 = m
    dx = (x1 - px) if px < x1 else (px - x2) if px > x2 else 0.0
    dy = (y1 - py) if py < y1 else (py - y2) if py > y2 else 0.0
    return dx * dx + dy * dy  # distancia^2 (evitamos sqrt)
