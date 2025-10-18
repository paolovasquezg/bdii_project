"""
RTREE with non-integer PK (VARCHAR) + sequential primary
- Create table with id VARCHAR(8) PRIMARY KEY using sequential
- Insert data without rtree
- Add rtree index and backfill
- Run geo query expecting correct ids
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

def run_sql(sql: str) -> dict:
    env = ENGINE.run(sql)
    print(json.dumps(env, indent=2, ensure_ascii=False))
    return env

def print_section(title: str):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

tbl = "rtree_varchar_pk"

print_section("1. Create table with VARCHAR PK (sequential)")
run_sql(f"DROP TABLE IF EXISTS {tbl};")
run_sql(f"""
    CREATE TABLE {tbl} (
        id VARCHAR(8) PRIMARY KEY,
        name VARCHAR(32),
        coords VARCHAR(64),
        INDEX (id) USING sequential
    );
""")

print_section("2. Insert rows (no rtree yet)")
run_sql(f"INSERT INTO {tbl} VALUES ('a1', 'A', '[0.0, 0.0]');")
run_sql(f"INSERT INTO {tbl} VALUES ('b2', 'B', '[1.0, 0.0]');")
run_sql(f"INSERT INTO {tbl} VALUES ('c3', 'C', '[5.0, 5.0]');")

print_section("3. Create rtree index (backfill)")
run_sql(f"CREATE INDEX ON {tbl} (coords) USING rtree;")

print_section("4. Run geo query expecting a1, b2")
env = run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.5);")
rows = env.get("results", [{}])[0].get("data", [])
ids = {r.get("id") for r in rows}
print("Got ids:", ids)
if ids == {"a1", "b2"}:
    print("✅ RTREE VARCHAR PK test passed!")
else:
    print("❌ Expected {'a1','b2'}, got:", ids)
