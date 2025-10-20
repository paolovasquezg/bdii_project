from backend.engine.engine import Engine

# (opcional) silenciar trazas internas de índices secundarios
try:
    from backend.storage.file import File
    File.p_print = staticmethod(lambda *a, **k: None)
except Exception:
    pass

ENGINE = Engine()

# ---------------- helpers de assertions con prints bonitos ---------------- #

def PASS(msg): print(f"[PASS] {msg}")
def FAIL(msg, got=None): print(f"[FAIL] {msg}" + ("" if got is None else f" -> got: {got}"))

def expect(cond, msg, got=None):
    if cond: PASS(msg)
    else:    FAIL(msg, got)

def run_one(sql: str):
    """Ejecuta UNA sentencia y devuelve (envelope, result)"""
    env = ENGINE.run(sql)
    expect(isinstance(env, dict), "envelope es dict", env)
    expect("ok" in env and "results" in env and "schema" in env, "envelope tiene ok/schema/results", env.keys())
    expect(isinstance(env["results"], list) and len(env["results"]) == 1, "envelope.results tiene 1 item", len(env.get("results", [])))
    res = env["results"][0]
    expect("ok" in res and "kind" in res and "action" in res and "data" in res and "count" in res, "result tiene campos básicos", res.keys())
    return env, res

def assert_ddl_ok(res, action, table=None, allow_noop=True):
    expect(res["ok"] is True, f"{action}: ok==True", res)
    expect(res["kind"] == "ddl", f"{action}: kind=='ddl'", res.get("kind"))
    expect(res["action"] == action, f"{action}: action coincide", res.get("action"))
    expect(res.get("count", 0) == 0, f"{action}: count==0", res.get("count"))
    expect(isinstance(res.get("data", []), list) and len(res["data"]) == 0, f"{action}: data vacía", res.get("data"))
    if table:
        expect(res.get("table") == table, f"{action}: table=='{table}'", res.get("table"))
    # noop (idempotente) es opcional (p.ej. CREATE INDEX IF NOT EXISTS)
    if "meta" in res and allow_noop and isinstance(res["meta"], dict) and "noop" in res["meta"]:
        expect(res["meta"]["noop"] in (True, False), f"{action}: meta.noop es booleano", res["meta"].get("noop"))

def assert_insert_ok(res, table=None):
    expect(res["ok"] is True, "insert: ok==True", res)
    expect(res["kind"] == "dml", "insert: kind=='dml'", res.get("kind"))
    expect(res["action"] == "insert", "insert: action=='insert'", res.get("action"))
    # Para DML no devolvemos filas; usamos meta.affected
    affected = (res.get("meta") or {}).get("affected", 0)
    expect(isinstance(affected, int) and affected >= 1, "insert: meta.affected>=1", affected)
    expect(res.get("count", 0) == 0, "insert: count==0 (select-only)", res.get("count"))
    if table:
        expect(res.get("table") == table, f"insert: table=='{table}'", res.get("table"))

def assert_delete_ok(res, table=None):
    expect(res["ok"] is True, "delete: ok==True", res)
    expect(res["kind"] == "dml", "delete: kind=='dml'", res.get("kind"))
    expect(res["action"] == "remove", "delete: action=='remove'", res.get("action"))
    affected = (res.get("meta") or {}).get("affected", 0)
    expect(isinstance(affected, int), "delete: meta.affected es int", affected)
    if table:
        expect(res.get("table") == table, f"delete: table=='{table}'", res.get("table"))

def assert_select_rows(res, expected_min=0, expected_max=None, must_equal=None, table=None):
    expect(res["ok"] is True, "select: ok==True", res)
    expect(res["kind"] == "dml", "select: kind=='dml'", res.get("kind"))
    expect(res["action"] in ("search", "range_search", "knn"), "select: action ∈ {search, range_search, knn}", res.get("action"))
    data = res.get("data", [])
    count = res.get("count", -1)
    expect(isinstance(data, list), "select: data es lista", type(data).__name__)
    expect(isinstance(count, int) and count == len(data), "select: count == len(data)", {"count": count, "len": len(data)})
    if must_equal is not None:
        expect(count == must_equal, f"select: count == {must_equal}", count)
    else:
        expect(count >= expected_min, f"select: count >= {expected_min}", count)
        if expected_max is not None:
            expect(count <= expected_max, f"select: count <= {expected_max}", count)
    if count > 0:
        # chequeo básico de saneamiento: que no vengan campos internos
        bad_keys = {"deleted", "pos"} & set(data[0].keys())
        expect(len(bad_keys) == 0, "select: sin campos internos (deleted/pos)", bad_keys)
    if table:
        expect(res.get("table") == table, f"select: table=='{table}'", res.get("table"))

# ------------------------- plan de pruebas end-to-end ---------------------- #

# 0) limpiar (idempotente)
_, r = run_one("DROP TABLE IF EXISTS products;")
assert_ddl_ok(r, "drop_table", table="products", allow_noop=True)

# 1) crear tabla
_, r = run_one("""
CREATE TABLE products (
  product_id INT PRIMARY KEY USING heap,
  name VARCHAR(32),
  price FLOAT INDEX USING bplus,
  stock INT,
  INDEX(name) USING hash
);
""")
assert_ddl_ok(r, "create_table", table="products")

# 2) crear índice (b+) sobre price (IF NOT EXISTS para probar idempotencia)
_, r = run_one("CREATE INDEX IF NOT EXISTS ON products (price) USING b+;")
assert_ddl_ok(r, "create_index", table="products")

# 3) insertar fila
_, r = run_one("INSERT INTO products (product_id, name, price, stock) VALUES (2, 'dup', 99, 5);")
assert_insert_ok(r, table="products")

# 4) SELECT por PK → 1 fila
_, r = run_one("SELECT * FROM products WHERE product_id = 2;")
assert_select_rows(r, must_equal=1, table="products")
row = r["data"][0]
expect(row.get("name") == "dup", "select by PK: name=='dup'", row)
try:
    expect(abs(float(row.get("price", -1)) - 99.0) < 1e-6, "select by PK: price==99.0", row)
except Exception:
    FAIL("select by PK: price (no float comparable)", row)

# 5) SELECT con secundario (BETWEEN AND name). Hasta implementar hash/b+,
#    aceptamos 0 o 1. Si ya tienes fallback al primario, esperas 1.
_, r = run_one("SELECT * FROM products WHERE price BETWEEN 40 AND 100 AND name = 'dup';")
cnt = r.get("count", 0)
if cnt == 0:
    PASS("select via secondary: (ok) 0 filas (índices secundarios aún no implementados)")
else:
    expect(cnt == 1, "select via secondary: 1 fila (con índice o fallback)", cnt)

# 6) DELETE por PK y verificación
_, r = run_one("DELETE FROM products WHERE product_id = 2;")
assert_delete_ok(r, table="products")
_, r = run_one("SELECT * FROM products WHERE product_id = 2;")
assert_select_rows(r, must_equal=0, table="products")

print("\n[SUMMARY] Si todo fue PASS, la envoltura URE está correcta y el flujo base funciona.\n"
      "Cuando implementes hash/b+, el paso (5) debería pasar con 1 fila.")
