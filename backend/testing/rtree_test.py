"""
RTREE • CSV + SQL smoke test (índice antes del import, sintaxis parser-correcta)
- Crea tabla (id, name, coords) con coords como JSON "[x, y]"
- Crea índice espacial USING rtree en (coords)
- Importa CSV (o fallback a INSERT por fila)
- Consulta geo: coords IN (POINT(x,y), r) y valida ids {1,2,3}
"""
import os, sys, csv, json

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

def ensure_csv(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","name","coords"])   # coords como JSON string "[x, y]"
        w.writerows([
            [1,"A","[0.0, 0.0]"],
            [2,"B","[1.0, 0.0]"],
            [3,"C","[0.0, 1.0]"],
            [4,"D","[1.0, 1.0]"],
            [5,"E","[2.0, 2.0]"],
            [6,"F","[5.0, 5.0]"],
            [7,"G","[-1.0, -1.0]"],
        ])

def import_csv_fallback_inserts(table: str, csv_path: str):
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            vals = []
            vals.append(str(int(row["id"])))
            vals.append("'" + row["name"].replace("'", "''") + "'")
            vals.append("'" + row["coords"].replace("'", "''") + "'")
            run_sql(f"INSERT INTO {table} VALUES ({', '.join(vals)});")

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
    tbl = "rtree_lugares_sql"
    csv_path = os.path.join(HERE, "_testdata", "csv", "places.csv")
    ensure_csv(csv_path)

    print_section("RTREE: create table + create index + import csv")
    run_sql(f"DROP TABLE IF EXISTS {tbl};")

    # 1) tabla
    run_sql(f"""
        CREATE TABLE {tbl} (
            id INT PRIMARY KEY,
            name VARCHAR(32),
            coords VARCHAR(64)
        );
    """)

    # 2) índice espacial (tu parser: paréntesis ANTES de USING)
    run_sql(f"CREATE INDEX ON {tbl} (coords) USING rtree;")

    # 3) import CSV (si tu 'FROM FILE' no inserta datos, hacemos fallback)

    clean = csv_path.replace('\\', '/')
    imp = run_sql(f"CREATE TABLE {tbl} FROM FILE '{clean}';")

    if not imp.get("ok", False):
        import_csv_fallback_inserts(tbl, csv_path)

    print_section("RTREE: consultas geoespaciales")
    env = run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.1);")
    expect_ids(env, {1, 2, 3})
    print("\n✅ RTREE test completed.")

if __name__ == "__main__":
    main()
