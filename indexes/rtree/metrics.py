# indexes/rtree/metrics.py
def avg_fill(nodes_count: int, total_entries: int, M: int) -> float:
    if nodes_count == 0: return 0.0
    return total_entries / (nodes_count * M)

def basic_stats(height: int, nodes: int, pages_r: int, pages_w: int, fill: float) -> dict:
    return {
        "height": height,
        "nodes": nodes,
        "pages_read": pages_r,
        "pages_written": pages_w,
        "avg_fill": round(fill, 3)
    }
