# bench_prim_sec_FIXED_PLUS_RTREE.py
# Igual al FIXED_PLUS, pero ACTIVAMOS pruebas espaciales (R-Tree) con KNN.
import json, csv, random, pathlib, datetime as dt

from backend.engine.engine import Engine


ENGINE = Engine()

def run_sql(sql: str):
    env = ENGINE.run(sql)
    if not env.get("ok", True):
        print("\n[SQL ERROR]\n", sql, "\n=>", json.dumps(env, indent=2, ensure_ascii=False))
    return env

def _collect_io_and_index_usage(env):
    total_r = total_w = 0; per_type = {}; idx_usage = []
    io_top = env.get("io")
    if isinstance(io_top, dict):
        if "total" in io_top and isinstance(io_top["total"], dict):
            total_r += int(io_top["total"].get("read_count",0) or 0)
            total_w += int(io_top["total"].get("write_count",0) or 0)
        for k,v in io_top.items():
            if isinstance(v, dict):
                per_type.setdefault(k, {"read":0,"write":0})
                per_type[k]["read"]  += int(v.get("read_count",0) or 0)
                per_type[k]["write"] += int(v.get("write_count",0) or 0)
    for res in env.get("results", []):
        meta = res.get("meta") or {}
        io = meta.get("io") or {}
        if "total" in io and isinstance(io["total"], dict):
            total_r += int(io["total"].get("read_count",0) or 0)
            total_w += int(io["total"].get("write_count",0) or 0)
        for k,v in io.items():
            if isinstance(v, dict):
                per_type.setdefault(k, {"read":0,"write":0})
                per_type[k]["read"]  += int(v.get("read_count",0) or 0)
                per_type[k]["write"] += int(v.get("write_count",0) or 0)
        iu = meta.get("index_usage") or []
        if isinstance(iu, list): idx_usage.extend(iu)
    if total_r==0 and total_w==0 and per_type:
        for v in per_type.values():
            total_r += v["read"]; total_w += v["write"]
    return total_r, total_w, per_type, idx_usage

def _extract_time(env):
    st = env.get("stats") or {}
    if st:
        for k in ("time_ms","parse_ms","plan_ms","exec_ms"):
            if k in st and st[k] is None: st[k] = 0.0
        return st
    for res in env.get("results", []):
        meta = res.get("meta") or {}
        if "time_ms" in meta:
            t = float(meta.get("time_ms") or 0.0)
            return {"time_ms": t, "exec_ms": t}
    return {"time_ms":0.0,"parse_ms":0.0,"plan_ms":0.0,"exec_ms":0.0}

def _format_idx_usage(iu):
    if not iu: return ""
    parts=[]
    for x in iu:
        parts.append(f"{x.get('where','?')}:{x.get('index','?')}({x.get('field','?')}):{x.get('op','?')}")
    return ";".join(parts)

def _split_io_data_index(active_primary, per_type, total_r, total_w):
    pr = per_type.get(active_primary, {"read":0,"write":0})
    data_r, data_w = pr["read"], pr["write"]
    idx_r  = max(0, total_r - data_r)
    idx_w  = max(0, total_w - data_w)
    return data_r, data_w, idx_r, idx_w

def take_stats(env, label, active_primary, extra=None):
    stats = _extract_time(env)
    total_r,total_w,per_type,idx_usage = _collect_io_and_index_usage(env)
    data_r,data_w,idx_r,idx_w = _split_io_data_index(active_primary, per_type, total_r, total_w)
    used_prim = any(u.get("where")=="primary" for u in idx_usage)
    used_sec  = any(u.get("where")=="secondary" for u in idx_usage)
    row = {
        "label": label,
        "time_ms_total": float(stats.get("time_ms") or 0.0),
        "time_ms_parse": float(stats.get("parse_ms") or 0.0),
        "time_ms_plan":  float(stats.get("plan_ms")  or 0.0),
        "time_ms_exec":  float(stats.get("exec_ms")  or stats.get("time_ms") or 0.0),
        "reads": int(total_r), "writes": int(total_w), "io_total": int(total_r+total_w),
        "io_data_reads": int(data_r), "io_data_writes": int(data_w),
        "io_index_reads": int(idx_r), "io_index_writes": int(idx_w),
        "idx_used_primary": int(1 if used_prim else 0),
        "idx_used_secondary": int(1 if used_sec else 0),
        "index_usage": _format_idx_usage(idx_usage),
    }
    if isinstance(extra, dict): row.update(extra)
    return row

# ========= Config =========
TABLE="bench_products"; PK="product_id"
CSV_PATH="/home/bianca/Documentos/bd2/testeo/backend/testing/benchmark/bd2_bench_products_1k.csv"  # <— CAMBIA

PRIMARY_METHODS=["heap","sequential","isam","bplus"]
SECONDARIES=[("name",["hash","bplus"]),("price",["bplus"]),("coords",["rtree"])]

N_LOOKUPS_EQ=100; N_LOOKUPS_RANGE=20
PK_RANGE_SPAN=150; PRICE_RANGE_PCT=0.05
KNN_K=10; KNN_QUERIES=20  # nº de consultas kNN

# ========= Dataset utils =========
def load_column_values(path, col):
    vals=[];
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f): vals.append(row[col])
    return vals

def bounds_int(path, col):
    lo=hi=None
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            v=int(row[col]); lo=v if lo is None else min(lo,v); hi=v if hi is None else max(hi,v)
    return lo,hi

def bounds_float(path, col):
    lo=hi=None
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            v=float(row[col]); lo=v if lo is None else min(lo,v); hi=v if hi is None else max(hi,v)
    return lo,hi

def coords_bounds(path, col="coords"):
    xs=[]; ys=[]
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            x,y = row[col].split(",")
            xs.append(float(x)); ys.append(float(y))
    return (min(xs), max(xs), min(ys), max(ys))

# ========= SQL helpers =========
def drop_table():                return run_sql(f"drop table {TABLE}")
def create_from_file(primary):   return run_sql(f"create table {TABLE} from file '{CSV_PATH}' using index {primary}({PK})")
def create_secondary(col, meth): return run_sql(f"create index idx_{TABLE}_{col}_{meth} on {TABLE}({col}) using {meth}")
def select_pk_eq(k):             return run_sql(f"select * from {TABLE} where {PK} = {int(k)}")
def select_pk_range(lo,hi):      return run_sql(f"select * from {TABLE} where {PK} between {int(lo)} and {int(hi)}")
def select_name_eq(val):
    safe=str(val).replace("'","''")
    return run_sql(f"select * from {TABLE} where name = '{safe}'")
def select_price_range(lo,hi):   return run_sql(f"select * from {TABLE} where price between {float(lo)} and {float(hi)}")
def select_knn_coords(x,y,k):    return run_sql(f"knn {TABLE} on coords point ({x},{y}) k {k}")

# ========= Preload =========
print("Preparando bounds...")
NAME_VALUES = load_column_values(CSV_PATH,"name")
PK_MIN,PK_MAX = bounds_int(CSV_PATH,PK)
PRICE_MIN,PRICE_MAX = bounds_float(CSV_PATH,"price")
XMIN,XMAX,YMIN,YMAX = coords_bounds(CSV_PATH,"coords")
PK_SAFE_SPAN = max(1, min(PK_RANGE_SPAN, max(1, PK_MAX-PK_MIN)))

random.seed(1337)
results=[]

for primary in PRIMARY_METHODS:
    drop_table()
    env = create_from_file(primary)
    results.append(take_stats(env,"import",primary,{"primary":primary,"target":"pk","op":"insert"}))

    # PK = igualdad
    for _ in range(N_LOOKUPS_EQ):
        k = random.randint(PK_MIN,PK_MAX)
        env = select_pk_eq(k)
        results.append(take_stats(env,"search/point",primary,{"primary":primary,"target":"pk","op":"search_eq"}))

    # PK = rango
    for _ in range(N_LOOKUPS_RANGE):
        lo = random.randint(PK_MIN, max(PK_MIN, PK_MAX-PK_SAFE_SPAN))
        hi = lo + PK_SAFE_SPAN
        env = select_pk_range(lo,hi)
        results.append(take_stats(env,"search/range",primary,{"primary":primary,"target":"pk","op":"search_range"}))

    # Secundarios
    for col, methods in SECONDARIES:
        for meth in methods:
            create_secondary(col, meth)

            if col=="name":
                for _ in range(N_LOOKUPS_EQ):
                    val = random.choice(NAME_VALUES)
                    env = select_name_eq(val)
                    results.append(take_stats(env,"search/point",primary,{
                        "primary":primary,"target":col,"secondary":meth,"op":"search_eq"
                    }))

            elif col=="price":
                span = max(0.01,(PRICE_MAX-PRICE_MIN)*PRICE_RANGE_PCT)
                for _ in range(N_LOOKUPS_RANGE):
                    center = random.uniform(PRICE_MIN,PRICE_MAX)
                    lo,hi = center-span, center+span
                    env = select_price_range(lo,hi)
                    results.append(take_stats(env,"search/range",primary,{
                        "primary":primary,"target":col,"secondary":meth,"op":"search_range"
                    }))

            elif col=="coords" and meth=="rtree":
                for _ in range(KNN_QUERIES):
                    x = random.uniform(XMIN,XMAX); y = random.uniform(YMIN,YMAX)
                    env = select_knn_coords(x,y,KNN_K)
                    results.append(take_stats(env,"search/knn",primary,{
                        "primary":primary,"target":"coords","secondary":"rtree","op":"knn","k":KNN_K
                    }))

    drop_table()

# Guardar
import pandas as pd
ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
outdir = pathlib.Path("bench_out"); outdir.mkdir(exist_ok=True)
df = pd.DataFrame(results)
csv_path = outdir / f"bench_primsec_{ts}.csv"
df.to_csv(csv_path, index=False)
print(f"Saved => {csv_path} (rows={len(df)})")
