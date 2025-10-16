# -*- coding: utf-8 -*-
"""
E2E RTREE:
- Índice creado ANTES del load y DESPUÉS del load (dos escenarios)
- Consulta por radio: coords IN (POINT(x,y), r)
- DELETE/REINSERT y re-evaluación
- Idempotencia en creación de índice
"""
import os, sys, csv
from test_utils import run_sql, assert_env_ok, get_rows, ok, bad

HERE = os.path.abspath(os.path.dirname(__file__))
CSV = os.path.join(HERE, "_testdata", "csv", "places_e2e.csv")

def ensure_places_csv(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","name","coords"])   # coords como string JSON "[x, y]"
        w.writerows([
            [1,"A","[0.0, 0.0]"],
            [2,"B","[1.0, 0.0]"],
            [3,"C","[0.0, 1.0]"],
            [4,"D","[1.0, 1.0]"],
            [5,"E","[2.0, 2.0]"],
            [6,"F","[5.0, 5.0]"],
            [7,"G","[-1.0, -1.0]"],
        ])

def scenario(tbl: str, create_index_before_load: bool):
    # Limpieza + tabla
    run_sql(f"DROP TABLE IF EXISTS {tbl};")
    assert_env_ok(run_sql(f"""
        CREATE TABLE {tbl} (
            id INT PRIMARY KEY,
            name VARCHAR(32),
            coords VARCHAR(64)
        );
    """), msg="create table")

    if create_index_before_load:
        assert_env_ok(run_sql(f"CREATE INDEX ON {tbl} (coords) USING rtree;"), msg="create index (pre)")
        ok("Índice espacial creado ANTES del load")
    else:
        ok("Índice espacial se creará DESPUÉS del load")

    # Import masivo
    clean = CSV.replace("\\", "/")
    assert_env_ok(run_sql(f"CREATE TABLE {tbl} FROM FILE '{clean}';"), msg="import places")

    # Si no existía índice aún, créalo ahora
    if not create_index_before_load:
        assert_env_ok(run_sql(f"CREATE INDEX IF NOT EXISTS ON {tbl} (coords) USING rtree;"), msg="create index (post)")
        ok("Índice espacial creado DESPUÉS del load")

    # Consulta por radio (centro 0,0, r=1.1) -> {1,2,3}
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.1);"), msg="geo radius")
    ids = sorted([int(x["id"]) for x in get_rows(r)])
    if ids != [1, 2, 3]:
        bad(f"Geo radius ids esperados [1,2,3], got {ids}")
    ok("Geo radius inicial OK")

    # DELETE uno (id=2) y verificar que cambia el resultado
    assert_env_ok(run_sql(f"DELETE FROM {tbl} WHERE id = 2;"), msg="delete id=2")
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.1);"), msg="geo radius post-delete")
    ids = sorted([int(x["id"]) for x in get_rows(r)])
    if ids != [1, 3]:
        bad(f"Geo radius tras delete -> [1,3], got {ids}")
    ok("Geo radius tras delete OK")

    # REINSERT id=2 y confirmar vuelve al set original
    assert_env_ok(run_sql(f"INSERT INTO {tbl} VALUES (2, 'B', '[1.0, 0.0]');"), msg="reinsert 2")
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE coords IN (POINT(0.0, 0.0), 1.1);"), msg="geo radius post-reinsert")
    ids = sorted([int(x["id"]) for x in get_rows(r)])
    if ids != [1, 2, 3]:
        bad(f"Geo radius tras reinsert -> [1,2,3], got {ids}")
    ok("Geo radius tras reinsert OK")

def main():
    ensure_places_csv(CSV)
    print("\n" + "="*70)
    print("E2E • RTREE (índice antes y después del load)")
    print("="*70)

    scenario("e2e_places_preidx", create_index_before_load=True)
    scenario("e2e_places_postidx", create_index_before_load=False)

    print("\n✅ E2E RTREE OK.")

if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
