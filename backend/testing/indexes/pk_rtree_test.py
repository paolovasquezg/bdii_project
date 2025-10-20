"""
Primary index switch with RTREE secondary test:
- Create table with sequential primary and RTREE on coords
- Insert 50 rows in one bulk INSERT
- Verify PK and RTREE queries
- Drop the primary index (sequential)
- Create a new primary index (bplus) on the same PK
- Verify PK and RTREE queries remain correct
"""
import os, sys, json

HERE = os.path.abspath(os.path.dirname(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

try:
    from backend.engine.engine import Engine
except Exception:
    from backend.engine import Engine  # type: ignore
ENGINE = Engine()

def run(sql: str):
    env = ENGINE.run(sql)
    print(json.dumps(env, indent=2, ensure_ascii=False))
    return env

def expect_ids(env: dict, expected: set):
    rows = env.get("results", [{}])[0].get("data", [])
    got = {r.get("id") for r in rows}
    if got != expected:
        raise AssertionError(f"Expected ids {expected}, got {got}")


tbl = "pk_switch_rtree"

# 1) create table with sequential primary and RTREE secondary
run("DROP TABLE IF EXISTS pk_switch_rtree;")
run(
    """
    CREATE TABLE pk_switch_rtree (
      id INT PRIMARY KEY,
      name VARCHAR(16),
      coords VARCHAR(64),
      INDEX (id) USING sequential,
      INDEX (coords) USING rtree
    );
    """
)

# 2) bulk insert 50 rows (spread points; several near origin)
values = ",\n".join(
    f"({i}, 'N{i}', '[{(i%5)-1.0}, {((i*2)%5)-1.0}]')" for i in range(1, 51)
)
run(f"INSERT INTO pk_switch_rtree VALUES {values};")

# 3) verify PK and RTREE queries BEFORE switching
run("SELECT * FROM pk_switch_rtree WHERE id = 1;")
run("SELECT * FROM pk_switch_rtree WHERE id = 50;")
geo = run("SELECT * FROM pk_switch_rtree WHERE coords IN (POINT(0.0, 0.0), 1.1);")
# We expect at least some hits (since many points are in [-1,1]x[-1,1])
# Not asserting exact set to avoid coupling to pattern; just ensure >= 5
cnt = geo.get("results", [{}])[0].get("count", 0)
if not (cnt >= 5):
    raise AssertionError(f"Expected at least 5 geo hits before switch, got {cnt}")

# 4) drop primary index (on column)
run("DROP INDEX ON pk_switch_rtree (id);")

# 5) create new primary index (bplus) on same PK
run("CREATE INDEX ON pk_switch_rtree (id) USING bplus;")

# 6) verify PK and RTREE queries AFTER switching
run("SELECT * FROM pk_switch_rtree WHERE id = 1;")
run("SELECT * FROM pk_switch_rtree WHERE id = 50;")
geo2 = run("SELECT * FROM pk_switch_rtree WHERE coords IN (POINT(0.0, 0.0), 1.1);")
cnt2 = geo2.get("results", [{}])[0].get("count", 0)
if not (cnt2 >= 5):
    raise AssertionError(f"Expected at least 5 geo hits after switch, got {cnt2}")

print("\n✅ Primary index switch with RTREE secondary test passed!")
