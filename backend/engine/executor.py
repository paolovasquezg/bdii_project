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

# -------- IO de forma consistente (también para DDL) -------- #
def ZERO_IO():
    z = {"read_count": 0, "write_count": 0}
    return {
        "heap": dict(z),
        "sequential": dict(z),
        "isam": dict(z),
        "bplus": dict(z),
        "hash": dict(z),
        "rtree": dict(z),
        "total": dict(z),
    }

# -------- helpers de (de)serialización segura del plan -------- #
_PRIMITIVES = (str, int, float, bool, type(None))

def _safe_plan(obj):
    """Convierte el plan a algo siempre serializable."""
    try:
        if isinstance(obj, _PRIMITIVES):
            return obj
        if isinstance(obj, dict):
            return {str(k): _safe_plan(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_safe_plan(v) for v in obj]
        return str(obj)
    except Exception:
        return str(obj)

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
    if action in ("create_table", "drop_table", "create_index", "drop_index", "create_table_from_file"):
        return "ddl"
    # DML (incluye consultas/selects)
    if action in ("insert", "remove", "search", "range search", "knn", "search_in", "geo_within", "select"):
        return "dml"
    return "query"

def _project_row(row, cols):
    if cols is None:
        return {k: v for k, v in row.items() if k not in INTERNAL_FIELDS}
    return {k: row.get(k) for k in cols if k not in INTERNAL_FIELDS}

def _as_point(v):
    if isinstance(v, dict): return (float(v.get("x")), float(v.get("y")))
    if isinstance(v, (list, tuple)) and len(v) >= 2: return (float(v[0]), float(v[1]))
    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
        try:
            j = _json.loads(v)
            if isinstance(j, (list, tuple)) and len(j) >= 2:
                return (float(j[0]), float(j[1]))
        except Exception: pass
    return None

def _eval_where(where, row):
    if not where: return True
    if "items" in where and "left" not in where:
        op = (where.get("op") or "").upper()
        vals = [_eval_where(w, row) for w in (where.get("items") or [])]
        return all(vals) if op == "AND" else any(vals)
    if {"left","op","right"} <= set(where.keys()):
        l = row.get(where["left"]); r = where["right"]; op = where["op"]
        if op in ("=", "=="): return l == r
        if op in ("!=", "<>"): return l != r
        try: lf, rf = float(l), float(r)
        except Exception: return False
        return (op == "<" and lf < rf) or (op == "<=" and lf <= rf) or \
               (op == ">" and lf > rf) or (op == ">=" and lf >= rf)
    if {"ident","lo","hi"} <= set(where.keys()):
        try:
            v = float(row.get(where["ident"]))
            return float(where["lo"]) <= v <= float(where["hi"])
        except Exception: return False
    if {"ident","items"} <= set(where.keys()):
        return row.get(where["ident"]) in (where.get("items") or [])
    if {"ident","center","radius"} <= set(where.keys()):
        p = _as_point(row.get(where["ident"]))
        if p is None: return False
        cx, cy = float(where["center"]["x"]), float(where["center"]["y"])
        rr = float(where["radius"])
        dx, dy = p[0]-cx, p[1]-cy
        return (dx*dx + dy*dy) ** 0.5 <= rr
    return False

# ---------------- mensajes consistentes en DML ---------------- #
def _fmt_rows(n: int) -> str:
    return f"{n} fila" if n == 1 else f"{n} filas"

def _fmt_vecinos(n: int) -> str:
    return f"{n} vecino" if n == 1 else f"{n} vecinos"

def _msg_for(action: str, *, count: int | None = None, affected: int | None = None) -> str:
    if action == "insert": return f"Insertadas {_fmt_rows(int(affected or 0))}."
    if action == "remove": return f"Eliminadas {_fmt_rows(int(affected or 0))}."
    if action in ("search", "select"): return f"Encontradas {_fmt_rows(int(count or 0))}."
    if action == "range search": return f"Encontradas {_fmt_rows(int(count or 0))} (rango)."
    if action == "geo_within": return f"Encontradas {_fmt_rows(int(count or 0))} (geo)."
    if action == "knn": return f"Encontrados {_fmt_vecinos(int(count or 0))} (kNN)."
    return ""

def ok_result(action, table=None, data=None, meta=None, message=None, t_ms: float = 0.0, plan=None):
    meta = meta or {}
    if data is None: data = []
    message = "" if message is None else message
    return {
        "ok": True,
        "kind": _kind_for(action),
        "action": action,
        "table": table,
        "count": len(data) if isinstance(data, list) else (meta.get("affected", 0) if isinstance(meta, dict) else 0),
        "data": data,
        "meta": {**meta, "time_ms": t_ms},
        "plan": plan,
        "message": message,
    }

def err_result(action, code, message, where="executor", detail=None, plan=None, t_ms: float = 0.0):
    message = "" if message is None else message
    return {
        "ok": False,
        "kind": _kind_for(action),
        "action": action,
        "error": {"code": code, "message": message, "where": where, "detail": detail},
        "count": 0,
        "data": [],
        "meta": {"time_ms": t_ms},
        "plan": plan or _safe_plan(detail.get("plan") if isinstance(detail, dict) else None)
    }

class Executor:
    def run(self, plans: List[Dict[str, Any]]):
        results = []
        overall_ok = True
        t0_all = perf_counter()

        for p in plans:
            action = p["action"]; table = p.get("table")
            plan_safe = _safe_plan(p)
            try:
                t0 = perf_counter()

                # ------------------------------- DDL ------------------------------- #
                if action == "create_table":
                    create_table(table, p["fields"])
                    results.append(ok_result(action, table, message="Tabla creada.",
                                             meta={"io": ZERO_IO(), "index_usage": []},
                                             t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                elif action == "create_index":
                    create_index(p["table"], p["column"], method=p.get("method") or "b+")
                    results.append(ok_result(action, table, message="Índice creado.",
                                             meta={"io": ZERO_IO(), "index_usage": []},
                                             t_ms=(perf_counter() - t0) * 1000, plan=plan_safe))

                elif action == "create_table_from_file":
                    table = p["table"]; path = p["path"]
                    F = File(table)

                    # detecto PK para ruta ISAM especial
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

                                    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                                        try:
                                            import json
                                            v = json.loads(v)
                                        except Exception:
                                            pass

                                    if v == "" or v is None:
                                        v = None
                                    elif t in ("int", "i"):
                                        v = int(v)
                                    elif t in ("float", "real", "double", "f"):
                                        v = float(v)
                                    elif t in ("bool", "boolean", "?"):
                                        v = str(v).strip().lower() in ("1", "true", "t", "yes", "y")
                                    rec[col] = v
                                recs.append(rec)
                        F.io_reset(); F.index_reset()
                        F.execute({"op": "build", "records": recs})
                        io = F.io_get(); idx = F.index_get()
                        results.append(ok_result(action, table, meta={"io": io, "index_usage": idx},
                                                message="Tabla creada desde CSV (ISAM).",
                                                t_ms=(perf_counter()-t0)*1000, plan=plan_safe))
                    else:
                        F.io_reset(); F.index_reset()
                        F.execute({"op": "import_csv", "path": path})
                        io = F.io_get(); idx = F.index_get()
                        results.append(ok_result(action, table, meta={"io": io, "index_usage": idx},
                                                message="Tabla creada desde CSV.",
                                                t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                elif action == "drop_table":
                    drop_table(table)
                    results.append(ok_result(action, table, message="Tabla eliminada.",
                                             meta={"io": ZERO_IO(), "index_usage": []},
                                             t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                elif action == "drop_index":
                    drop_index(p.get("table"), p.get("column") or p.get("name"))
                    results.append(ok_result(action, table, message="Índice eliminado.",
                                             meta={"io": ZERO_IO(), "index_usage": []},
                                             t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                # ------------------------------- DML ------------------------------- #
                elif action in ("insert","remove","search","range search","knn","search_in","geo_within","select"):
                    F = File(table)

                    if action == "search_in":
                        field = p["field"]; items = list(p.get("items") or [])
                        acc: List[Dict[str, Any]] = []
                        F.io_reset(); F.index_reset()
                        for v in items:
                            rr = F.execute({"op": "search", "field": field, "value": v})
                            if isinstance(rr, list):
                                acc.extend(rr)
                        # dedup opcional por "id"
                        seen = set(); merged = []
                        for r in acc:
                            if isinstance(r, dict):
                                k = r.get("id", None)
                                if k is not None:
                                    if k in seen: continue
                                    seen.add(k)
                            merged.append(r)
                        io = F.io_get(); idx = F.index_get()
                        data, cnt = _sanitize_rows(merged)
                        results.append(ok_result("search", table, data=data,
                                                 meta={"io": io, "index_usage": idx},
                                                 message=_msg_for("search", count=cnt),
                                                 t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                    elif action == "geo_within":
                        field = p["field"]; center = p["center"]; radius = p["radius"]

                        def _bbox(c, r):
                            cx, cy = float(c["x"]), float(c["y"])
                            rr = float(r)
                            return {"xmin": cx - rr, "ymin": cy - rr, "xmax": cx + rr, "ymax": cy + rr}

                        F.io_reset(); F.index_reset()
                        rows = None

                        # 1) RTREE círculo nativo
                        try:
                            r1 = F.execute({"op": "rtree_within_circle", "field": field,
                                            "center": center, "radius": radius})
                            rows = r1 if r1 else None
                        except Exception:
                            rows = None

                        # 2) RTREE bbox + refine si no hubo resultados
                        if not rows:
                            try:
                                rect = _bbox(center, radius)
                                candidates = F.execute({"op": "rtree_range", "field": field, "rect": rect}) or []
                                if candidates:
                                    cx, cy, rr = float(center["x"]), float(center["y"]), float(radius)
                                    filt = []
                                    for r in candidates:
                                        if isinstance(r, dict):
                                            v = r.get(field)
                                            if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                                                try: v = _json.loads(v)
                                                except Exception: pass
                                            if isinstance(v, dict):
                                                px, py = float(v.get("x")), float(v.get("y"))
                                            elif isinstance(v, (list, tuple)) and len(v) >= 2:
                                                px, py = float(v[0]), float(v[1])
                                            else:
                                                continue
                                            dx, dy = (px - cx), (py - cy)
                                            if (dx*dx + dy*dy) ** 0.5 <= rr:
                                                filt.append(r)
                                    rows = filt if filt else None
                            except Exception:
                                rows = rows or None

                        # 3) Si aún nada, intentar op genérica (si tu File la implementa)
                        if not rows:
                            try:
                                r3 = F.execute({"op": "geo_within", "field": field,
                                                "center": center, "radius": radius})
                                rows = r3 if r3 else None
                            except Exception:
                                rows = None

                        # 4) Último recurso: heap-scan
                        if not rows:
                            try:
                                rows = _heap_geo_scan(table, field, center, radius) or []
                            except Exception:
                                rows = []

                        io = F.io_get(); idx = F.index_get()
                        data, cnt = _sanitize_rows(rows or [])
                        results.append(ok_result("geo_within", table, data=data,
                                                 meta={"io": io, "index_usage": idx},
                                                 message=_msg_for("geo_within", count=cnt),
                                                 t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                    elif action == "insert":
                        rec = p.get("record")
                        if p.get("record_is_positional"):
                            meta = table_meta_path(table)
                            relation, _ = get_json(str(meta), 2)
                            order = [f["name"] for f in relation] if isinstance(relation, list) else list(relation.keys())
                            rec = dict(zip(order, rec))
                        F.io_reset(); F.index_reset()
                        res = F.execute({"op": "insert", "record": rec})
                        affected = 1
                        if isinstance(res, dict) and "affected" in res:
                            affected = int(res["affected"])
                        elif isinstance(res, list):
                            affected = len(res)
                        io = F.io_get(); idx = F.index_get()
                        if affected == 0:
                            # Intentar identificar la PK para mensaje más claro
                            try:
                                meta = table_meta_path(table)
                                relation, _ = get_json(str(meta), 2)
                                pk_name = None
                                if isinstance(relation, dict):
                                    for col, spec in relation.items():
                                        if isinstance(spec, dict) and (spec.get("key") == "primary"):
                                            pk_name = col; break
                                else:
                                    for spec in (relation or []):
                                        if isinstance(spec, dict) and (spec.get("key") == "primary"):
                                            pk_name = spec.get("name"); break
                                pk_val = rec.get(pk_name) if (pk_name and isinstance(rec, dict)) else None
                                msg = f"PK duplicada ({pk_name}={pk_val})" if pk_name else "PK duplicada"
                            except Exception:
                                msg = "PK duplicada"
                            results.append(err_result(action, code="DUPLICATE_KEY", message=msg,
                                                      detail={"plan": p}, plan=plan_safe,
                                                      t_ms=(perf_counter()-t0)*1000))
                            overall_ok = False
                        else:
                            results.append(ok_result(action, table,
                                                     meta={"affected": affected, "io": io, "index_usage": idx},
                                                     message=_msg_for("insert", affected=affected),
                                                     t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                    elif action == "remove":
                        F.io_reset(); F.index_reset()
                        res = F.execute({"op": "remove", "field": p["field"], "value": p["value"]})
                        affected = 0
                        if isinstance(res, list): affected = len(res)
                        elif isinstance(res, dict) and "affected" in res: affected = res["affected"]
                        io = F.io_get(); idx = F.index_get()
                        results.append(ok_result(action, table,
                                                 meta={"affected": affected, "io": io, "index_usage": idx},
                                                 message=_msg_for("remove", affected=affected),
                                                 t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                    elif action == "select":
                        F.io_reset(); F.index_reset()
                        rows = F.execute({"op": "scan"}) or []
                        where = p.get("where")
                        if where:
                            rows = [r for r in rows if isinstance(r, dict) and _eval_where(where, r)]
                        cols = p.get("columns")
                        projected = [_project_row(r, cols) for r in rows if isinstance(r, dict)]
                        data, cnt = _sanitize_rows(projected)
                        io = F.io_get(); idx = F.index_get()
                        results.append(ok_result(action, table, data=data,
                                                 meta={"io": io, "index_usage": idx},
                                                 message=_msg_for("select", count=cnt),
                                                 t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                    else:
                        # search / range search / knn “directos”
                        payload = {k: v for k, v in p.items() if k not in ("action", "table", "post_filter")}
                        payload["op"] = action
                        F.io_reset(); F.index_reset()
                        rows = F.execute(payload) or []
                        pf = p.get("post_filter")
                        if pf and isinstance(rows, list):
                            rows = [r for r in rows if isinstance(r, dict) and r.get(pf["field"]) == pf["value"]]
                        data, cnt = _sanitize_rows(rows)
                        io = F.io_get(); idx = F.index_get()
                        results.append(ok_result(action, table, data=data,
                                                 meta={"io": io, "index_usage": idx},
                                                 message=_msg_for(action, count=cnt),
                                                 t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                else:
                    results.append(err_result(action, "UNSUPPORTED_ACTION",
                                              f"Acción no soportada: {action}",
                                              detail={"plan": p}, plan=plan_safe, t_ms=(perf_counter()-t0)*1000))
                    overall_ok = False

            except Exception as ex:
                results.append(err_result(action, "EXEC_ERROR", str(ex),
                                          detail={"plan": p}, plan=plan_safe, t_ms=(perf_counter()-t0)*1000))
                overall_ok = False

        total_ms = (perf_counter() - t0_all) * 1000.0
        return {
            "ok": overall_ok,
            "schema": "bd2.v1",
            "results": results,
            "warnings": [],
            "stats": {"time_ms": total_ms}
        }