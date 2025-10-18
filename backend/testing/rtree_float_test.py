"""
RTREE with FLOAT PK + sequential primary
- Create table with id FLOAT PRIMARY KEY using sequential
- Insert data without rtree
- Add rtree index and backfill
- Run geo query expecting correct float ids
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

tbl = "rtree_float_pk"

print_section("1. Create table with FLOAT PK (sequential)")
run_sql(f"DROP TABLE IF EXISTS {tbl};")
run_sql(f"""
    CREATE TABLE {tbl} (
        id FLOAT PRIMARY KEY,
        name VARCHAR(32),
        coords VARCHAR(64),
        INDEX (id) USING sequential
    );
""")

print_section("2. Insert rows (no rtree yet)")
run_sql(f"INSERT INTO {tbl} VALUES (1.0, 'A', '[0.0, 0.0]');")
run_sql(f"INSERT INTO {tbl} VALUES (2.0, 'B', '[1.0, 0.0]');")
run_sql(f"INSERT INTO {tbl} VALUES (3.0, 'C', '[5.0, 5.0]');")

print_section("3. Create rtree index (backfill)")
run_sql(f"CREATE INDEX ON {tbl} (coords) USING rtree;")

print_section("4. Run geo query expecting 1.0, 2.0")
env = run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.5);")
rows = env.get("results", [{}])[0].get("data", [])
ids = {r.get("id") for r in rows}
print("Got ids:", ids)
if ids == {1.0, 2.0}:
    print("✅ RTREE FLOAT PK test passed!")
else:
    print("❌ Expected {1.0, 2.0}, got:", ids)
