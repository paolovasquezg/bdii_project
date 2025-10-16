# -*- coding: utf-8 -*-
"""
E2E: CRUD + secundarios (hash/b+) en 3 backends de PK (heap, sequential, isam).
Cubre:
- DDL (DROP/CREATE TABLE)
- Secundarios: price (b+), INDEX(name) USING hash   [si parser lo permite]
- INSERTs (dataset + reinsert), DELETE, SELECT por PK/rango/segundarios
- Error esperado: PK duplicada (sin imprimir ok:false)
- Idempotencia: CREATE INDEX IF NOT EXISTS (si aplica)

Diseño: NO imprimimos env JSON salvo debug puntual. Si algo falla → sys.exit(1).
"""
import os, sys
from test_utils import (
    run_sql, assert_env_ok, get_rows, expect_error, ensure_products_csv, ok, bad
)

HERE = os.path.abspath(os.path.dirname(__file__))
CSV = os.path.join(HERE, "_testdata", "csv", "products.csv")

PK_METHODS = ["heap", "sequential", "isam"]   # agrega "bplus" si tu parser/ejecutor lo soporta

def create_table(pk: str, tbl: str):
    # Intento con secundarios declarativos (hash/b+). Si falla, caemos a tabla simple.
    ddl = f"""
        CREATE TABLE {tbl} (
            product_id INT PRIMARY KEY USING {pk},
            name VARCHAR(32),
            price FLOAT INDEX USING bplus,
            stock INT,
            INDEX(name) USING hash
        );
    """
    env = run_sql(ddl)
    if not env.get("ok", False):
        # Fallback: sin secundarios en DDL (los crearemos después, o haremos SCAN)
        ddl2 = f"""
            CREATE TABLE {tbl} (
                product_id INT PRIMARY KEY USING {pk},
                name VARCHAR(32),
                price FLOAT,
                stock INT
            );
        """
        res = assert_env_ok(run_sql(ddl2), msg=f"create_table {tbl}")
        ok(f"CREATE TABLE {tbl} (sin secundarios declarativos)")
        return False
    assert_env_ok(env, msg=f"create_table {tbl}")
    ok(f"CREATE TABLE {tbl} (con secundarios declarativos)")
    return True

def ensure_indexes(tbl: str):
    # Índices explícitos por si el DDL no los dejó
    env1 = run_sql(f"CREATE INDEX IF NOT EXISTS ON {tbl} (price) USING b+;")
    # puede no soportar IF NOT EXISTS; aceptamos OK o NOOP en meta
    if env1.get("ok", False):
        ok("CREATE INDEX (price) USING b+")
    env2 = run_sql(f"CREATE INDEX IF NOT EXISTS ON {tbl} (name) USING hash;")
    if env2.get("ok", False):
        ok("CREATE INDEX (name) USING hash")

def import_data(tbl: str):
    ensure_products_csv(CSV)
    clean = CSV.replace("\\", "/")
    env = run_sql(f"CREATE TABLE {tbl} FROM FILE '{clean}';")  # tu sintaxis especial
    # Si tu import no inserta, hacemos fallback a INSERT por fila desde test_utils en otros tests;
    # Aquí exigimos OK para que sea E2E de import masivo.
    assert_env_ok(env, msg="import FROM FILE")
    ok("IMPORT ok (FROM FILE)")

def verify_queries(tbl: str):
    # SELECT por PK
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE product_id = 3;"), msg="select pk")
    rows = get_rows(r)
    if len(rows) != 1 or rows[0].get("name") != "Charlie":
        bad("select pk=3 -> 1 fila 'Charlie'")

    ok("SELECT por PK ok")

    # Rango por PK
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE product_id BETWEEN 2 AND 4;"), msg="range pk")
    rows = get_rows(r)
    ids = sorted([int(x["product_id"]) for x in rows])
    if ids != [2, 3, 4]:
        bad("range pk [2..4] -> ids 2,3,4")
    ok("RANGE por PK ok")

    # Secundario hash (nombre)
    # Aceptamos fallback a SCAN; evaluamos solo el resultado.
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE name = 'Bravo';"), msg="hash name")
    rows = get_rows(r)
    if len(rows) != 1 or int(rows[0]["product_id"]) != 2:
        bad("hash(name) o SCAN -> 1 fila id=2")
    ok("SELECT por name=Bravo ok")

    # Secundario b+ (price BETWEEN ...). Puede ser con índice o SCAN.
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE price BETWEEN 10 AND 26;"), msg="b+ price")
    ids = sorted([int(x["product_id"]) for x in get_rows(r)])
    # Del CSV: ids esperados = 1 (10.5), 3 (15.0), 4 (25.0)
    if ids != [1, 3, 4]:
        bad("b+ (price) o SCAN -> ids 1,3,4")
    ok("SELECT price BETWEEN ok")

def verify_dml(tbl: str):
    # INSERT normal
    assert_env_ok(run_sql(f"INSERT INTO {tbl} VALUES (6, 'Foxtrot', 33.3, 9);"), msg="insert 6")
    ok("INSERT id=6 ok")

    # Duplicado (error esperado, no imprimimos JSON)
    expect_error(f"INSERT INTO {tbl} (product_id, name, price, stock) VALUES (3, 'dup', 9.9, 1);")
    ok("INSERT duplicado rechaza (esperado)")

    # DELETE + verificación
    assert_env_ok(run_sql(f"DELETE FROM {tbl} WHERE product_id = 2;"), msg="delete id=2")
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE product_id = 2;"), msg="post-delete")
    if len(get_rows(r)) != 0:
        bad("DELETE id=2 -> luego 0 filas")
    ok("DELETE + verificación ok")

    # Reinsert + verificación
    assert_env_ok(run_sql(f"INSERT INTO {tbl} VALUES (2, 'Bravo', 20.0, 15);"), msg="reinsert 2")
    r = assert_env_ok(run_sql(f"SELECT * FROM {tbl} WHERE product_id = 2;"), msg="post-reinsert")
    if len(get_rows(r)) != 1:
        bad("REINSERT id=2 -> 1 fila")
    ok("REINSERT + verificación ok")

def main():
    any_fail = False
    for pk in PK_METHODS:
        tbl = f"e2e_products_{pk}"
        print("\n" + "="*70)
        print(f"E2E • PK USING {pk} • tabla {tbl}")
        print("="*70)

        # 0) limpiar
        run_sql(f"DROP TABLE IF EXISTS {tbl};")

        # 1) crear tabla (+secundarios si parser lo permite)
        with_ok_sec = create_table(pk, tbl)

        # 2) crear índices explícitos si hizo fallback
        if not with_ok_sec:
            ensure_indexes(tbl)

        # 3) cargar datos (import masivo)
        import_data(tbl)

        # 4) consultas por PK/segundarios
        verify_queries(tbl)

        # 5) DML (insert/dup/delete/reinsert)
        verify_dml(tbl)

        print(f"✔ E2E {tbl} OK")

    print("\n✅ E2E CRUD+Índices: TODOS los backends probados OK.")

if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)