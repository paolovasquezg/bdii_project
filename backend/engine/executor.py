from typing import Any, Dict, List
from time import perf_counter
import json as _json
import struct
from backend.catalog.ddl import create_table, create_index, drop_table, drop_index
from backend.storage.file import File
from backend.catalog.catalog import table_meta_path, get_json
from backend.core.utils import build_format
from backend.core.record import Record

INTERNAL_FIELDS = {"deleted", "pos", "slot"}

# --- Fallback: heap-scan geo dentro de un radio ---
def _heap_geo_scan(table, field, center, radius):
    meta = table_meta_path(table)
    relation, indexes = get_json(str(meta), 2)
    pidx = indexes.get("primary", {})
    if pidx.get("index") != "heap":
        return []
    heap_file = pidx["filename"]
    with open(heap_file, "rb") as hf:
        slen = struct.unpack("I", hf.read(4))[0]
        schema = _json.loads(hf.read(slen).decode("utf-8"))
        fmt = build_format(schema)
        rec_size = struct.calcsize(fmt)
        cx, cy, rr = float(center["x"]), float(center["y"]), float(radius)
        end = hf.seek(0, 2); hf.seek(4 + slen)
        out = []
        while hf.tell() < end:
            chunk = hf.read(rec_size)
            if not chunk or len(chunk) < rec_size: break
            rec = Record.unpack(chunk, fmt, schema).fields
            if rec.get("deleted"):
                continue
            v = rec.get(field)
            if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                try: v = _json.loads(v)
                except Exception: continue
            if isinstance(v, (list, tuple)) and len(v) >= 2:
                px, py = float(v[0]), float(v[1])
            elif isinstance(v, dict):
                px, py = float(v.get("x")), float(v.get("y"))
            else:
                continue
            dx, dy = px - cx, py - cy
            if dx*dx + dy*dy <= rr*rr:
                out.append(rec)
        return out

def _sanitize_rows(rows):
    if not isinstance(rows, list):
        return [], 0
    out = []
    for r in rows:
        if isinstance(r, dict):
            out.append({k: v for k, v in r.items() if k not in INTERNAL_FIELDS})
        else:
            out.append(r)
    return out, len(out)

def _kind_for(action: str) -> str:
    # DDL
    if action in (
        "create_table", "drop_table",
        "create_index", "drop_index",
        "create_table_from_file"
    ):
        return "ddl"

    # DML (incluye consultas/selects)
    if action in (
        "insert", "remove",
        "search", "range search", "knn",
        "search_in", "geo_within",
        "select"
    ):
        return "dml"

    # Fallback
    return "query"

def _project_row(row, cols):
    if cols is None:
        return {k: v for k, v in row.items() if k not in INTERNAL_FIELDS}
    return {k: row.get(k) for k in cols if k not in INTERNAL_FIELDS}

def _as_point(v):
    if isinstance(v, dict):
        return (float(v.get("x")), float(v.get("y")))
    if isinstance(v, (list, tuple)) and len(v) >= 2:
        return (float(v[0]), float(v[1]))
    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
        try:
            j = _json.loads(v)
            if isinstance(j, (list, tuple)) and len(j) >= 2:
                return (float(j[0]), float(j[1]))
        except Exception:
            pass
    return None

def _eval_where(where, row):
    if not where:
        return True

    # BoolExpr: {"op": "AND"/"OR", "items":[...]}
    if "items" in where and "left" not in where:
        op = (where.get("op") or "").upper()
        vals = [_eval_where(w, row) for w in (where.get("items") or [])]
        return all(vals) if op == "AND" else any(vals)

    # Comparison: {"left": <campo>, "op": "=", "<", ... , "right": <valor>}
    if {"left","op","right"} <= set(where.keys()):
        l = row.get(where["left"])
        r = where["right"]
        op = where["op"]
        if op in ("=", "=="): return l == r
        if op in ("!=", "<>"): return l != r
        try:
            lf, rf = float(l), float(r)
        except Exception:
            return False
        return (op == "<" and lf < rf) or (op == "<=" and lf <= rf) or \
               (op == ">" and lf > rf) or (op == ">=" and lf >= rf)

    # Between: {"ident":c, "lo":a, "hi":b}
    if {"ident","lo","hi"} <= set(where.keys()):
        try:
            v = float(row.get(where["ident"]))
            return float(where["lo"]) <= v <= float(where["hi"])
        except Exception:
            return False

    # InList: {"ident":c, "items":[...]}
    if {"ident","items"} <= set(where.keys()):
        return row.get(where["ident"]) in (where.get("items") or [])

    # GeoWithin: {"ident":c, "center":{"x":..,"y":..}, "radius":r}
    if {"ident","center","radius"} <= set(where.keys()):
        p = _as_point(row.get(where["ident"]))
        if p is None: return False
        cx, cy = float(where["center"]["x"]), float(where["center"]["y"])
        rr = float(where["radius"])
        dx, dy = p[0]-cx, p[1]-cy
        return (dx*dx + dy*dy) ** 0.5 <= rr

    return False

def ok_result(action, table=None, data=None, meta=None, message=None, t_ms: float = 0.0):
    meta = meta or {}
    if data is None: data = []
    return {
        "ok": True,
        "kind": _kind_for(action),
        "action": action,
        "table": table,
        "count": len(data) if isinstance(data, list) else (meta.get("affected", 0) if isinstance(meta, dict) else 0),
        "data": data,
        "meta": {**meta, "time_ms": t_ms},
        "plan": None,
        "message": message,
    }

def err_result(action, code, message, where="executor", detail=None):
    return {
        "ok": False,
        "kind": _kind_for(action),
        "action": action,
        "error": {"code": code, "message": message, "where": where, "detail": detail},
        "count": 0,
        "data": [],
        "meta": {}
    }

def _euclid(p, q):
    try:
        return ((float(p[0]) - float(q[0]))**2 + (float(p[1]) - float(q[1]))**2) ** 0.5
    except Exception:
        return float("inf")

class Executor:
    def run(self, plans: List[Dict[str, Any]]):
        results = []
        overall_ok = True
        t0_all = perf_counter()

        for p in plans:
            action = p["action"]; table = p.get("table")
            try:
                t0 = perf_counter()

                if action == "create_table":
                    create_table(table, p["fields"])
                    results.append(ok_result(action, table, message="Tabla creada.", t_ms=(perf_counter()-t0)*1000))

                elif action == "create_index":
                    create_index(p["table"], p["column"], method=p.get("method") or "b+")
                    results.append(
                        ok_result(action, table, message="Índice creado.", t_ms=(perf_counter() - t0) * 1000))

                elif action == "create_table_from_file":
                    table = p["table"]
                    path = p["path"]

                    F = File(table)
                    prim = (F.indexes or {}).get("primary", {})
                    pk_kind = (prim.get("index") or "").lower()

                    if pk_kind == "isam":
                        import csv, json as _json
                        recs = []
                        with open(path, newline="", encoding="utf-8") as f:
                            r = csv.DictReader(f)
                            for row in r:
                                rec = {}
                                for col, spec in F.relation.items():
                                    v = row.get(col)
                                    t = str(spec.get("type") or "").lower()

                                    # Parse JSON para campos tipo lista (ej. coords: "[x,y]")
                                    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                                        try:
                                            import json
                                            v = json.loads(v)
                                        except Exception:
                                            pass

                                    # Cast básico por tipo
                                    if v == "" or v is None:
                                        v = None
                                    elif t in ("int", "i"):
                                        v = int(v)
                                    elif t in ("float", "real", "double", "f"):
                                        v = float(v)
                                    elif t in ("bool", "boolean", "?"):
                                        v = str(v).strip().lower() in ("1", "true", "t", "yes", "y")
                                    # strings/varchar se dejan como están
                                    rec[col] = v

                                recs.append(rec)

                        # Esto inicializa el índice (root + hojas + páginas) y luego inserta.
                        F.execute({"op": "build", "records": recs})
                        results.append(ok_result(action, table))

                    else:
                        # Heap/Sequential u otros → import fila a fila
                        F.execute({"op": "import_csv", "path": path})
                        results.append(ok_result(action, table))

                elif action == "drop_table":
                    drop_table(table)
                    results.append(ok_result(action, table, message="Tabla eliminada.", t_ms=(perf_counter()-t0)*1000))

                elif action == "drop_index":
                    drop_index(p.get("table"), p.get("column") or p.get("name"))
                    results.append(ok_result(action, table, message="Índice eliminado.", t_ms=(perf_counter()-t0)*1000))

                elif action in ("insert", "remove", "search", "range search", "knn",
                                "search_in", "geo_within", "select"):
                    F = File(table)

                    if action == "search_in":
                        field = p["field"]; items = list(p.get("items") or [])
                        acc: List[Dict[str, Any]] = []
                        for v in items:
                            rr = F.execute({"op": "search", "field": field, "value": v})
                            if isinstance(rr, list):
                                acc.extend(rr)
                        # opcional: dedup por clave "id" si existe
                        seen = set(); merged = []
                        for r in acc:
                            if isinstance(r, dict):
                                k = r.get("id", None)
                                if k is not None:
                                    if k in seen: continue
                                    seen.add(k)
                            merged.append(r)
                        data, _ = _sanitize_rows(merged)
                        results.append(ok_result("search", table, data=data, t_ms=(perf_counter()-t0)*1000))

                    elif action == "geo_within":
                        field = p["field"]
                        center = p["center"]
                        radius = p["radius"]

                        F = File(table)

                        def _bbox(c, r):
                            cx, cy = float(c["x"]), float(c["y"])
                            rr = float(r)
                            return {"xmin": cx - rr, "ymin": cy - rr, "xmax": cx + rr, "ymax": cy + rr}

                        rows = None

                        # 1) Intento nativo R-tree (círculo)
                        try:
                            rows = F.execute({
                                "op": "rtree_within_circle",
                                "field": field,
                                "center": center,
                                "radius": radius
                            })
                        except Exception:
                            rows = None

                        # 2) Intento con bounding box (R-tree) + refine por distancia
                        if rows is None:
                            try:
                                rect = _bbox(center, radius)
                                candidates = F.execute({
                                    "op": "rtree_range",
                                    "field": field,
                                    "rect": rect
                                }) or []
                                # refine: distancia euclidiana al centro
                                cx, cy = float(center["x"]), float(center["y"])
                                rr = float(radius)
                                filt = []
                                for r in candidates:
                                    if isinstance(r, dict):
                                        v = r.get(field)
                                        if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                                            try:
                                                v = _json.loads(v)
                                            except Exception:
                                                pass
                                        if isinstance(v, dict):
                                            px, py = float(v.get("x")), float(v.get("y"))
                                        elif isinstance(v, (list, tuple)) and len(v) >= 2:
                                            px, py = float(v[0]), float(v[1])
                                        else:
                                            continue
                                        dx, dy = (px - cx), (py - cy)
                                        if (dx * dx + dy * dy) ** 0.5 <= rr:
                                            filt.append(r)
                                rows = filt
                            except Exception:
                                rows = None

                        # 3) Intento genérico storage (por si implementas otro nombre)
                        if rows is None:
                            try:
                                rows = F.execute({
                                    "op": "geo_within",
                                    "field": field,
                                    "center": center,
                                    "radius": radius
                                })
                            except Exception:
                                rows = None

                        # 4) Fallback naive: scan + filtro
                        if rows is None:
                            rows = []
                            try:
                                allrows = F.execute({"op": "scan"}) or []
                                cx, cy = float(center["x"]), float(center["y"])
                                rr = float(radius)
                                for r in allrows:
                                    if isinstance(r, dict):
                                        v = r.get(field)
                                        if isinstance(v, dict):
                                            px, py = float(v.get("x")), float(v.get("y"))
                                        elif isinstance(v, (list, tuple)) and len(v) >= 2:
                                            px, py = float(v[0]), float(v[1])
                                        else:
                                            continue
                                        dx, dy = (px - cx), (py - cy)
                                        if (dx * dx + dy * dy) ** 0.5 <= rr:
                                            rows.append(r)
                            except Exception:
                                rows = []

                        if not rows:
                            try:
                                rows = _heap_geo_scan(table, field, center, radius)
                            except Exception:
                                rows = []

                        data, _ = _sanitize_rows(rows)
                        results.append(ok_result("search", table, data=data, t_ms=(perf_counter() - t0) * 1000))

                    elif action == "insert":
                        table = p["table"]
                        rec = p.get("record")
                        if p.get("record_is_positional"):
                            from backend.catalog.catalog import table_meta_path, get_json
                            meta = table_meta_path(table)
                            relation, _ = get_json(str(meta), 2)
                            order = [f["name"] for f in relation] if isinstance(relation, list) else list(
                                relation.keys())
                            rec = dict(zip(order, rec))

                        F = File(table)
                        res = F.execute({"op": "insert", "record": rec})

                        # Determinar filas afectadas (mínimo 1 si no hay retorno)
                        affected = 1
                        if isinstance(res, dict) and "affected" in res:
                            affected = int(res["affected"])
                        elif isinstance(res, list):
                            affected = len(res)

                        results.append(ok_result(action, table, meta={"affected": affected}))

                    elif action == "remove":
                        res = F.execute({"op": "remove", "field": p["field"], "value": p["value"]})
                        affected = 0
                        if isinstance(res, list): affected = len(res)
                        elif isinstance(res, dict) and "affected" in res: affected = res["affected"]
                        results.append(ok_result(action, table, meta={"affected": affected}, message="OK", t_ms=(perf_counter()-t0)*1000))

                    elif action == "select":
                        F = File(table)

                        # 1) Trae todas las filas (SELECT * FROM ...)
                        rows = F.execute({"op": "scan"}) or []

                        # 2) WHERE (si viene)
                        where = p.get("where")
                        if where:
                            rows = [r for r in rows if isinstance(r, dict) and _eval_where(where, r)]

                        # 3) Proyección (lista de columnas o '*')
                        cols = p.get("columns")  # None => '*'
                        projected = []
                        for r in rows:
                            if isinstance(r, dict):
                                projected.append(_project_row(r, cols))

                        data, _ = _sanitize_rows(projected)
                        results.append(ok_result(action, table, data=data, t_ms=(perf_counter() - t0) * 1000))

                    else:
                        # Passthrough: search / range search / knn
                        payload = {k: v for k, v in p.items() if k not in ("action", "table", "post_filter")}
                        payload["op"] = action
                        rows = F.execute(payload) or []
                        # post-filter (ej. BETWEEN AND name='x')
                        pf = p.get("post_filter")
                        if pf and isinstance(rows, list):
                            rows = [r for r in rows if isinstance(r, dict) and r.get(pf["field"]) == pf["value"]]
                        data, _ = _sanitize_rows(rows)
                        results.append(ok_result(action, table, data=data, t_ms=(perf_counter()-t0)*1000))

                else:
                    results.append(err_result(action, "UNSUPPORTED_ACTION", f"Acción no soportada: {action}", detail={"plan": p}))
                    overall_ok = False

            except Exception as ex:
                results.append(err_result(action, "EXEC_ERROR", str(ex), detail={"plan": p}))
                overall_ok = False

        total_ms = (perf_counter() - t0_all) * 1000.0
        return {"ok": overall_ok, "schema": "bd2.v1", "results": results, "warnings": [], "stats": {"time_ms": total_ms}}
