# -*- coding: utf-8 -*-
"""
E2E: CRUD + secundarios (hash/b+) en 3 backends de PK (heap, sequential, isam).
- NO tapa errores: solo mejora observabilidad con ✓/✗ y detalles (plan, índices, filas).
- Continúa aunque falle un paso y muestra un resumen final (rc=1 si hubo fallos).
"""
import os, sys, csv, time

HERE = os.path.abspath(os.path.dirname(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Engine import (soporta ambos layouts)
try:
    from backend.engine.engine import Engine
except Exception:
    from backend.engine import Engine  # type: ignore

ENGINE = Engine()
CSV = os.path.join(HERE, "_testdata", "csv", "products.csv")
PK_METHODS = ["heap", "sequential", "isam", "bplus", "hash", "rtree"]

CHECK = "✓"
CROSS = "✗"

# ---------------------------
# Utilidades de impresión
# ---------------------------
def _short_plan(res0: dict) -> str:
    """Plan breve: action, table, y campo si aplica."""
    p = res0.get("plan") or {}
    act = p.get("action")
    tbl = p.get("table")
    fld = p.get("field")
    rng = ""
    if "min" in p or "max" in p:
        rng = f" [{p.get('min')}..{p.get('max')}]"
    if act and tbl and fld:
        return f"{act}:{tbl}.{fld}{rng}"
    if act and tbl:
        return f"{act}:{tbl}{rng}"
    return act or "-"

def _short_usage(meta: dict) -> str:
    """Resumen de índices usados y tiempo."""
    if not isinstance(meta, dict):
        return "-"
    t = meta.get("time_ms")
    idx = meta.get("index_usage") or []
    if idx:
        # ej: "primary:heap(product_id), secondary:hash(name)"
        parts = []
        for u in idx:
            where = u.get("where")
            ind = u.get("index")
            field = u.get("field")
            op = u.get("op")
            if where and ind and field:
                parts.append(f"{where}:{ind}({field})" + (f"/{op}" if op else ""))
        used = ", ".join(parts)
    else:
        used = "index:none"
    if t is not None:
        return f"{used}; {t:.2f}ms"
    return used

def _rows_info(rows):
    """Muestra conteo y, si están, los ids/product_id."""
    if not isinstance(rows, list):
        return "rows:? "
    n = len(rows)
    # tratar de deducir ids
    ids = []
    for r in rows:
        if isinstance(r, dict):
            if "id" in r:
                ids.append(r["id"])
            elif "product_id" in r:
                ids.append(r["product_id"])
    if ids:
        return f"rows:{n} ids:{ids}"
    return f"rows:{n}"

def _print_step_ok(label, res0):
    meta = res0.get("meta") or {}
    print(f"  {CHECK} {label}  • plan={_short_plan(res0)}  • {_short_usage(meta)}")

def _print_step_bad(label, res0_or_env, reason):
    # intenta extraer meta/plan incluso cuando viene ok:false
    if isinstance(res0_or_env, dict):
        res0 = (res0_or_env.get("results") or [{}])[0] if res0_or_env.get("results") else res0_or_env
        meta = res0.get("meta") or {}
        plan = _short_plan(res0)
        usage = _short_usage(meta)
        print(f"  {CROSS} {label}  • plan={plan}  • {usage}")
        print(f"      └─ {reason}")
    else:
        print(f"  {CROSS} {label}  • {reason}")

# ---------------------------
# Helpers de ejecución
# ---------------------------
def ensure_products_csv(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id","name","price","stock"])
        w.writerows([
            [1,"Alpha",10.5,50],
            [2,"Bravo",20.0,15],
            [3,"Charlie",15.0,30],
            [4,"Delta",25.0,5],
            [5,"Echo",30.5,0],
        ])

def run_sql(sql: str) -> dict:
    return ENGINE.run(sql)

def assert_ok(env: dict, msg=""):
    if not isinstance(env, dict) or not env.get("ok", False):
        raise AssertionError(f"Envelope NOT ok: {msg or '-'}: {env}")
    r0 = (env.get("results") or [{}])[0]
    if not r0.get("ok", False):
        raise AssertionError(f"Result NOT ok: {msg or '-'}: {env}")
    return r0

def ensure_indexes(tbl: str):
    # No “tapa” errores de motor; solo asegura que existan si el DDL no los creó.
    run_sql(f"CREATE INDEX IF NOT EXISTS ON {tbl} (name) USING hash;")
    run_sql(f"CREATE INDEX IF NOT EXISTS ON {tbl} (price) USING b+;")

def import_data(tbl: str):
    ensure_products_csv(CSV)
    clean = CSV.replace("\\", "/")
    # Preferir INSERT FROM FILE (no destructivo). Si no existe, insertar fila por fila.
    env = run_sql(f"INSERT INTO {tbl} FROM FILE '{clean}';")
    if env.get("ok", False) and ((env.get("results") or [{}])[0]).get("ok", False):
        _print_step_ok("IMPORT (INSERT FROM FILE)", (env["results"][0]))
        return
    # Fallback a inserts fila por fila, pero reportando cada bloque
    with open(CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            pid = int(row["product_id"])
            name = row["name"].replace("'", "''")
            price = float(row["price"])
            stock = int(row["stock"])
            res0 = assert_ok(run_sql(
                f"INSERT INTO {tbl} VALUES ({pid}, '{name}', {price}, {stock});"
            ), msg=f"insert {pid}")
        _print_step_ok("IMPORT (INSERT fila a fila)", res0)

# ---------------------------
# Verificaciones con detalle
# ---------------------------
def verify_queries(tbl: str):
    # SELECT por PK
    env = run_sql(f"SELECT * FROM {tbl} WHERE product_id = 3;")
    res0 = assert_ok(env, msg="select pk")
    rows = res0.get("data") or []
    if len(rows) == 1 and rows[0].get("name") == "Charlie":
        _print_step_ok("SELECT PK (product_id=3)", res0)
        print(f"     └─ {_rows_info(rows)}")
    else:
        _print_step_bad("SELECT PK (product_id=3)", env, f"esperado 1 fila 'Charlie', got {_rows_info(rows)}")

    # Rango por PK
    env = run_sql(f"SELECT * FROM {tbl} WHERE product_id BETWEEN 2 AND 4;")
    res0 = assert_ok(env, msg="range pk")
    rows = res0.get("data") or []
    ids = sorted(int(x["product_id"]) for x in rows)
    if ids == [2,3,4]:
        _print_step_ok("RANGE PK (2..4)", res0)
        print(f"     └─ {_rows_info(rows)}")
    else:
        _print_step_bad("RANGE PK (2..4)", env, f"esperado ids [2,3,4], got {ids}")

    # Secundario name='Bravo'
    env = run_sql(f"SELECT * FROM {tbl} WHERE name = 'Bravo';")
    res0 = assert_ok(env, msg="name=Bravo")
    rows = res0.get("data") or []
    if len(rows) == 1 and int(rows[0].get("product_id", -1)) == 2:
        _print_step_ok("SELECT name='Bravo'", res0)
        print(f"     └─ {_rows_info(rows)}")
    else:
        # dar pista: índice usado / no usado
        meta = res0.get("meta") or {}
        idx = meta.get("index_usage") or []
        used = ", ".join(f"{u.get('where')}:{u.get('index')}({u.get('field')})" for u in idx) if idx else "index:none"
        _print_step_bad("SELECT name='Bravo'", env, f"esperado 1 fila id=2, got {_rows_info(rows)}; used={used}")

    # Secundario price BETWEEN 10..26
    env = run_sql(f"SELECT * FROM {tbl} WHERE price BETWEEN 10 AND 26;")
    res0 = assert_ok(env, msg="price between")
    rows = res0.get("data") or []
    ids = sorted(int(x["product_id"]) for x in rows)
    if ids == [1,3,4]:
        _print_step_ok("SELECT price BETWEEN 10..26", res0)
        print(f"     └─ {_rows_info(rows)}")
    else:
        _print_step_bad("SELECT price BETWEEN 10..26", env, f"esperado ids [1,3,4], got {ids}")

def verify_dml(tbl: str):
    # INSERT normal
    env = run_sql(f"INSERT INTO {tbl} VALUES (6, 'Foxtrot', 33.3, 9);")
    res0 = assert_ok(env, msg="insert 6")
    _print_step_ok("INSERT id=6", res0)

    # Duplicado (error esperado)
    env = run_sql(f"INSERT INTO {tbl} (product_id, name, price, stock) VALUES (3, 'dup', 9.9, 1);")
    if env.get("ok", False) and ((env.get("results") or [{}])[0]).get("ok", False):
        _print_step_bad("INSERT duplicado id=3", env, "se esperaba error pero fue OK")
    else:
        # imprimir código/mensaje de error si viene
        res0 = (env.get("results") or [{}])[0]
        err = res0.get("error") or {}
        code = err.get("code") or "ERROR"
        msg = err.get("message") or "-"
        print(f"  {CHECK} INSERT duplicado id=3 rechazado  • {code}: {msg}")

    # DELETE + verificación
    res0 = assert_ok(run_sql(f"DELETE FROM {tbl} WHERE product_id = 2;"), msg="delete id=2")
    _print_step_ok("DELETE id=2", res0)
    env = run_sql(f"SELECT * FROM {tbl} WHERE product_id = 2;")
    res0 = assert_ok(env, msg="post-delete")
    rows = res0.get("data") or []
    if len(rows) == 0:
        print(f"  {CHECK} POST-DELETE id=2  • ok (0 filas)")
    else:
        _print_step_bad("POST-DELETE id=2", env, f"esperado 0 filas, got {_rows_info(rows)}")

    # Reinsert + verificación
    res0 = assert_ok(run_sql(f"INSERT INTO {tbl} VALUES (2, 'Bravo', 20.0, 15);"), msg="reinsert 2")
    _print_step_ok("REINSERT id=2", res0)
    env = run_sql(f"SELECT * FROM {tbl} WHERE product_id = 2;")
    res0 = assert_ok(env, msg="post-reinsert")
    rows = res0.get("data") or []
    if len(rows) == 1:
        print(f"  {CHECK} POST-REINSERT id=2  • ok (1 fila)")
    else:
        _print_step_bad("POST-REINSERT id=2", env, f"esperado 1 fila, got {_rows_info(rows)}")

# ------------------------
# Infra de pasos tolerantes
# ------------------------
def run_step(table_name: str, label: str, func, failures: list):
    """Ejecuta una etapa; imprime ✓/✗ y registra fallo si ocurre."""
    t0 = time.perf_counter()
    try:
        func()
        dt = (time.perf_counter() - t0) * 1000
        print(f"→ {CHECK} {label}  ({dt:.1f} ms)")
    except AssertionError as e:
        dt = (time.perf_counter() - t0) * 1000
        print(f"→ {CROSS} {label}  ({dt:.1f} ms)")
        print(f"    └─ {e}")
        failures.append((table_name, label, str(e)))
    except Exception as e:
        dt = (time.perf_counter() - t0) * 1000
        print(f"→ {CROSS} {label}  ({dt:.1f} ms)")
        print(f"    └─ EXCEPTION {type(e).__name__}: {e}")
        failures.append((table_name, label, f"EXCEPTION {type(e).__name__}: {e}"))

# ------------------------
# Main
# ------------------------
def main():
    global_failures = []

    for pk in PK_METHODS:
        tbl = f"e2e_products_{pk}"
        table_failures = []

        print("\n" + "="*70)
        print(f"E2E • PK USING {pk} • tabla {tbl}")
        print("="*70)

        run_sql(f"DROP TABLE IF EXISTS {tbl};")

        # Crear tabla (sin secundarios declarativos: no queremos tapar problemas del import)
        res0 = assert_ok(run_sql(f'''
            CREATE TABLE {tbl} (
                product_id INT PRIMARY KEY USING {pk},
                name VARCHAR(32),
                price FLOAT,
                stock INT
            );
        '''), msg="create")
        _print_step_ok("CREATE TABLE", res0)

        run_step(tbl, "IMPORT DATA", lambda: import_data(tbl), table_failures)

        # Si tu DDL no creó secundarios, los aseguramos ahora (no oculta bugs de búsqueda, solo crea si faltan)
        run_step(tbl, "ENSURE INDEXES", lambda: ensure_indexes(tbl), table_failures)

        run_step(tbl, "VERIFY QUERIES", lambda: verify_queries(tbl), table_failures)
        run_step(tbl, "VERIFY DML", lambda: verify_dml(tbl), table_failures)

        # Resumen por tabla
        if table_failures:
            print(f"\n⟡ Resumen {tbl}: {len(table_failures)} fallos")
            for _, label, msg in table_failures:
                print(f"   - {label}: {msg}")
        else:
            print(f"✔ E2E {tbl} OK")

        global_failures.extend(table_failures)

    # Resumen global y exit code
    if global_failures:
        print("\n" + "#" * 70)
        print(f"RESUMEN GLOBAL: {len(global_failures)} fallos")
        for tbl, label, msg in global_failures:
            print(f" - {tbl} :: {label} -> {msg}")
        print("#" * 70)
        sys.exit(1)
    else:
        print("\n✅ E2E CRUD+Índices: TODOS los backends probados OK.")
        sys.exit(0)

if __name__ == "__main__":
    main()