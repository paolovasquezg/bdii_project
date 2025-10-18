"""
RTREE Backfill Test
- Create table with NO primary index initially, just rtree on coords
- Insert data
- Create sequential primary index on id (triggers backfill)
- Run geo query to verify rtree still works
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

def expect_ids(env: dict, expected_ids: set):
    try:
        res = env["results"][0]
        rows = res.get("data", [])
        got = {int(r["id"]) for r in rows if "id" in r}
    except Exception:
        got = set()
    if got != expected_ids:
        raise AssertionError(f"Geo query expected ids {expected_ids}, got {got}")
    print("  ✓ Geo results OK:", sorted(got))

def main():
    tbl = "rtree_backfill_test"
    
    print_section("Step 1: Create table with heap primary, insert data with rtree")
    run_sql(f"DROP TABLE IF EXISTS {tbl};")
    
    # Create table with heap primary (default) and rtree on coords
    run_sql(f"""
        CREATE TABLE {tbl} (
            id INT PRIMARY KEY,
            name VARCHAR(32),
            coords VARCHAR(64),
            INDEX (coords) USING rtree
        );
    """)
    
    print_section("Step 2: Insert data")
    # Insert test data
    run_sql(f"INSERT INTO {tbl} VALUES (1, 'A', '[0.0, 0.0]');")
    run_sql(f"INSERT INTO {tbl} VALUES (2, 'B', '[1.0, 0.0]');")
    run_sql(f"INSERT INTO {tbl} VALUES (3, 'C', '[0.0, 1.0]');")
    run_sql(f"INSERT INTO {tbl} VALUES (4, 'D', '[1.0, 1.0]');")
    run_sql(f"INSERT INTO {tbl} VALUES (5, 'E', '[2.0, 2.0]');")
    run_sql(f"INSERT INTO {tbl} VALUES (6, 'F', '[5.0, 5.0]');")
    run_sql(f"INSERT INTO {tbl} VALUES (7, 'G', '[-1.0, -1.0]');")
    
    print_section("Step 3: Test geo query with heap primary")
    env = run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.1);")
    expect_ids(env, {1, 2, 3})
    
    # Now test the real scenario: create a NEW table with sequential primary
    tbl2 = "rtree_backfill_test2"
    
    print_section("Step 4: Create NEW table with sequential primary and existing data")
    run_sql(f"DROP TABLE IF EXISTS {tbl2};")
    
    # Create table with sequential primary from the start
    run_sql(f"""
        CREATE TABLE {tbl2} (
            id INT PRIMARY KEY,
            name VARCHAR(32),
            coords VARCHAR(64),
            INDEX (id) USING sequential
        );
    """)
    
    # Insert data
    run_sql(f"INSERT INTO {tbl2} VALUES (1, 'A', '[0.0, 0.0]');")
    run_sql(f"INSERT INTO {tbl2} VALUES (2, 'B', '[1.0, 0.0]');")
    run_sql(f"INSERT INTO {tbl2} VALUES (3, 'C', '[0.0, 1.0]');")
    run_sql(f"INSERT INTO {tbl2} VALUES (4, 'D', '[1.0, 1.0]');")
    run_sql(f"INSERT INTO {tbl2} VALUES (5, 'E', '[2.0, 2.0]');")
    run_sql(f"INSERT INTO {tbl2} VALUES (6, 'F', '[5.0, 5.0]');")
    run_sql(f"INSERT INTO {tbl2} VALUES (7, 'G', '[-1.0, -1.0]');")
    
    print_section("Step 5: Create rtree index on existing data (triggers backfill)")
    # This should trigger backfill_secondary for the rtree
    run_sql(f"CREATE INDEX ON {tbl2} (coords) USING rtree;")
    
    print_section("Step 6: Test geo query after backfill")
    env = run_sql(f"SELECT * FROM {tbl2} WHERE coords IN (POINT(0.0, 0.0), 1.1);")
    expect_ids(env, {1, 2, 3})
    
    print("\n✅ RTREE backfill test completed successfully!")

if __name__ == "__main__":
    main()
