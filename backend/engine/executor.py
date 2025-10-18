from typing import Any, Dict, List
from time import perf_counter
import json as _json
import struct
import csv as _csv
import os
from backend.catalog.ddl import create_table, create_index, drop_table, drop_index
from backend.storage.file import File
from backend.catalog.catalog import table_meta_path, get_json
from backend.core.utils import build_format
from backend.core.record import Record
from backend.storage.indexes.heap import HeapFile

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

# --- helper: detectar nombre de PK de una tabla ---
def _detect_pk_name(table: str) -> str | None:
    try:
        meta = table_meta_path(table)
        relation, _ = get_json(str(meta), 2)
        if isinstance(relation, dict):
            for col, spec in relation.items():
                if isinstance(spec, dict) and (spec.get("key") == "primary"):
                    return col
        else:
            for spec in (relation or []):
                if isinstance(spec, dict) and (spec.get("key") == "primary"):
                    return spec.get("name")
    except Exception:
        pass
    return None

# --- Fallback: heap-scan geo dentro de un radio ---
def _heap_geo_scan(table, field, center, radius):
    meta = table_meta_path(table)
    relation, indexes = get_json(str(meta), 2)
    pidx = indexes.get("primary", {})
    if pidx.get("index") != "heap":
        return []
    heap_file = pidx["filename"]
    with open(heap_file, "rb") as hf:
        slen = struct.unpack("<I", hf.read(4))[0]
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
    if action in ("insert", "remove", "search", "range_search", "knn", "search_in", "geo_within", "select"):
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

def _infer_type(values):
    """Inferencia simple: int > float > bool > varchar."""
    saw_float = saw_int = saw_bool = False
    for v in values:
        if v is None or str(v).strip() == "":
            continue
        s = str(v).strip()
        sl = s.lower()
        if sl in ("true", "false"):
            saw_bool = True
            continue
        try:
            int(s)
            saw_int = True
            continue
        except Exception:
            pass
        try:
            float(s)
            saw_float = True
            continue
        except Exception:
            pass
        # Cualquier otra cosa = varchar
        return "varchar"
    if saw_float: return "float"
    if saw_int:   return "int"
    if saw_bool:  return "bool"
    return "varchar"


def _infer_fields_from_csv(path, index_column=None, index_method=None, sample_rows=200):
    with open(path, newline="", encoding="utf-8") as f:
        r = _csv.DictReader(f)
        headers = r.fieldnames or []
        buckets = {h: [] for h in headers}
        for i, row in enumerate(r):
            for h in headers:
                buckets[h].append(row.get(h))
            if i + 1 >= sample_rows:
                break

    fields = []
    for h in headers:
        t = _infer_type(buckets[h])

        fdef = {"name": h, "type": t}

        # >>> CLAVE DEL FIX: dar tamaño a varchar <<<
        if t == "varchar":
            # longitud observada (muestral) con margen de seguridad
            try:
                maxlen = max((len(str(v)) for v in buckets[h] if v is not None), default=0)
            except Exception:
                maxlen = 0
            # límites razonables: al menos 8, tope 255 (ajústalo si tu motor permite más)
            size = max(8, min(maxlen or 32, 255))
            # guarda ambos por compatibilidad
            fdef["length"] = size
            fdef["size"] = size

        # PK si corresponde
        if index_column and str(index_column) == h:
            fdef["key"] = "primary"
            if index_method:
                fdef["index"] = index_method

        fields.append(fdef)

    return fields

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
    if action == "range_search": return f"Encontradas {_fmt_rows(int(count or 0))} (rango)."
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
        elapsed_ms = 0.0

        for p in plans:
            action = p["action"]; table = p.get("table")
            plan_safe = _safe_plan(p)
            try:
                t0 = perf_counter()
                before_len = len(results)

                # ------------------------------- DDL ------------------------------- #
                if action == "create_table":
                    create_table(table, p["fields"])
                    results.append(ok_result(action, table, message="Tabla creada.",
                                             meta={"io": ZERO_IO(), "index_usage": []},
                                             t_ms=(perf_counter()-t0)*1000, plan=plan_safe))

                elif action == "create_index":
                    create_index(p["table"], p["column"], method=p.get("method") or "bplus")
                    results.append(ok_result(action, table, message="Índice creado.",
                                             meta={"io": ZERO_IO(), "index_usage": []},
                                             t_ms=(perf_counter() - t0) * 1000, plan=plan_safe))

                elif action == "create_table_from_file":
                    table = p["table"]
                    path = p["path"]
                    idx_method = (p.get("index_method") or None)
                    idx_column = p.get("index_column")

                    meta_path = table_meta_path(table)
                    exists = False
                    try:
                        # si el json de metadatos existe, la tabla existe
                        _ = get_json(str(meta_path), 2)
                        exists = True
                    except Exception:
                        exists = False

                    # Si ya existe y NO es IF NOT EXISTS -> error (semántica de CREATE TABLE)
                    #if exists and not p.get("if_not_exists", False):
                    #    results.append(err_result(action, "TABLE_EXISTS",
                    #                              f"La tabla '{table}' ya existe.",
                    #                              detail={"plan": p},
                    #                              plan=plan_safe, t_ms=(perf_counter() - t0) * 1000))
                    #    overall_ok = False
                    #    continue

                    # Si NO existe, la creamos infiriendo el esquema del CSV
                    if not exists:
                        fields = _infer_fields_from_csv(path, index_column=idx_column, index_method=idx_method)
                        create_table(table, fields)

                    # Importamos/Construimos según el tipo de PK
                    F = File(table)

                    prim = (F.indexes or {}).get("primary", {})
                    pk_kind = (prim.get("index") or "").lower()

                    if pk_kind == "isam":
                        # build ISAM en una sola pasada
                        recs = []
                        with open(path, newline="", encoding="utf-8") as f:
                            r = _csv.DictReader(f)
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
                        F.io_reset()
                        F.index_reset()
                        F.execute({"op": "build", "records": recs})
                        io = F.io_get()
                        idx = F.index_get()
                        results.append(ok_result(action, table, meta={"io": io, "index_usage": idx},
                                                 message="Tabla creada desde CSV (ISAM).",
                                                 t_ms=(perf_counter() - t0) * 1000, plan=plan_safe))
                    else:
                        # heap / sequential / bplus / hash: import CSV
                        F.io_reset()
                        F.index_reset()
                        F.execute({"op": "import_csv", "path": path})
                        io = F.io_get()
                        idx = F.index_get()
                        results.append(ok_result(action, table, meta={"io": io, "index_usage": idx},
                                                 message="Tabla creada desde CSV.",
                                                 t_ms=(perf_counter() - t0) * 1000, plan=plan_safe))

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
                elif action in ("insert","remove","search","range_search","knn","search_in","geo_within","select"):
                    F = File(table)

                    if action == "search_in":
                        field = p["field"]; items = list(p.get("items") or [])
                        acc: List[Dict[str, Any]] = []
                        F.io_reset(); F.index_reset()
                        for v in items:
                            rr = F.execute({"op": "search", "field": field, "value": v})
                            if isinstance(rr, list):
                                acc.extend(rr)
                        pk_name = _detect_pk_name(table)
                        seen = set(); merged = []
                        for r in acc:
                            if isinstance(r, dict) and pk_name:
                                k = r.get(pk_name, None)
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

                        try:
                            r1 = F.execute({"op": "rtree_within_circle", "field": field,
                                            "center": center, "radius": radius})
                            rows = r1 if r1 else None
                        except Exception:
                            rows = None

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

                        if not rows:
                            try:
                                r3 = F.execute({"op": "geo_within", "field": field,
                                                "center": center, "radius": radius})
                                rows = r3 if r3 else None
                            except Exception:
                                rows = None

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
                        # ---- NUEVO: INSERT INTO t FROM FILE 'path' ----
                        if p.get("from_file"):
                            path = p.get("from_file")
                            F.io_reset(); F.index_reset()
                            res = F.execute({"op": "import_csv", "path": path}) or {}
                            affected = int(res.get("count", 0)) if isinstance(res, dict) else 0
                            io = F.io_get(); idx = F.index_get()
                            results.append(ok_result("insert", table,
                                                     meta={"affected": affected, "io": io, "index_usage": idx},
                                                     message=_msg_for("insert", affected=affected),
                                                     t_ms=(perf_counter()-t0)*1000, plan=plan_safe))
                            continue  # pasa al siguiente plan

                        # ---- INSERT de una fila ----
                        rec = p.get("record")
                        if p.get("record_is_positional"):
                            meta = table_meta_path(table)
                            relation, _ = get_json(str(meta), 2)
                            order = [f["name"] for f in relation] if isinstance(relation, list) else list(relation.keys())
                            rec = dict(zip(order, rec))

                        pk_name = _detect_pk_name(table)

                        F.io_reset(); F.index_reset()
                        try:
                            res = F.execute({"op": "insert", "record": rec})
                            affected = 0
                            if isinstance(res, dict) and "affected" in res:
                                affected = int(res["affected"])
                            elif isinstance(res, list):
                                affected = len(res)
                            else:
                                affected = 1

                            io = F.io_get(); idx = F.index_get()
                            if affected <= 0:
                                msg = f"PK duplicada ({pk_name}={rec.get(pk_name)})" if pk_name else "PK duplicada"
                                results.append(err_result(action, code="DUPLICATE_KEY", message=msg,
                                                          detail={"plan": p}, plan=plan_safe,
                                                          t_ms=(perf_counter()-t0)*1000))
                                overall_ok = False
                            else:
                                results.append(ok_result(action, table,
                                                         meta={"affected": affected, "io": io, "index_usage": idx},
                                                         message=_msg_for("insert", affected=affected),
                                                         t_ms=(perf_counter()-t0)*1000, plan=plan_safe))
                        except Exception as ex:
                            dup_confirmed = False
                            try:
                                if pk_name and isinstance(rec, dict) and pk_name in rec:
                                    F.io_reset(); F.index_reset()
                                    already = F.execute({"op": "search", "field": pk_name, "value": rec[pk_name]}) or []
                                    if isinstance(already, list) and len(already) >= 1:
                                        dup_confirmed = True
                                        msg = f"PK duplicada ({pk_name}={rec.get(pk_name)})"
                                        results.append(err_result(action, code="DUPLICATE_KEY", message=msg,
                                                                  detail={"plan": p}, plan=plan_safe,
                                                                  t_ms=(perf_counter()-t0)*1000))
                                        overall_ok = False
                            except Exception:
                                pass
                            if not dup_confirmed:
                                results.append(err_result(action, "EXEC_ERROR", str(ex),
                                                          detail={"plan": p}, plan=plan_safe,
                                                          t_ms=(perf_counter()-t0)*1000))
                                overall_ok = False

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
                        F.io_reset()
                        F.index_reset()
                        where = p.get("where")
                        cols = p.get("columns")

                        def _emit_ok(rows):
                            normalized = []
                            for r in rows or []:
                                if isinstance(r, tuple) and len(r) >= 1:
                                    normalized.append(r[0])
                                elif isinstance(r, dict):
                                    normalized.append(r)
                            projected = [_project_row(r, cols) for r in normalized if isinstance(r, dict)]
                            data, cnt = _sanitize_rows(projected)
                            io = F.io_get()
                            idx = F.index_get()
                            results.append(ok_result("select", table, data=data,
                                                     meta={"io": io, "index_usage": idx},
                                                     message=_msg_for("select", count=cnt),
                                                     t_ms=(perf_counter() - t0) * 1000, plan=plan_safe))

                        if where is None:
                            rows = F.execute({"op": "get_all"}) or []
                            _emit_ok(rows)
                        elif {"left", "op", "right"} <= set(where.keys()) and where.get("op") in ("=", "=="):
                            rows = F.execute({"op": "search", "field": where["left"], "value": where["right"]}) or []
                            _emit_ok(rows)
                        elif {"ident", "lo", "hi"} <= set(where.keys()):
                            rows = F.execute({"op": "range_search", "field": where["ident"],
                                              "min": where["lo"], "max": where["hi"]}) or []
                            _emit_ok(rows)
                        else:
                            results.append(err_result("select", "UNSUPPORTED_SELECT",
                                                      "Forma de SELECT no soportada (usa search/range).",
                                                      detail={"plan": p}, plan=plan_safe,
                                                      t_ms=(perf_counter() - t0) * 1000))
                            overall_ok = False

                    else:
                        # search / range search / knn “directos”
                        payload = {k: v for k, v in p.items() if k not in ("action", "table", "post_filter")}
                        payload["op"] = action
                        F.io_reset()
                        F.index_reset()
                        rows = F.execute(payload) or []
                        pf = p.get("post_filter")
                        if pf and isinstance(rows, list):
                            rows = [r for r in rows if isinstance(r, dict) and r.get(pf["field"]) == pf["value"]]
                        data, cnt = _sanitize_rows(rows)
                        io = F.io_get();
                        idx = F.index_get()
                        results.append(ok_result(action, table, data=data,
                                                 meta={"io": io, "index_usage": idx},
                                                 message=_msg_for(action, count=cnt),
                                                 t_ms=(perf_counter() - t0) * 1000, plan=plan_safe))
                else:
                    results.append(err_result(action, "UNSUPPORTED_ACTION",
                                              f"Acción no soportada: {action}",
                                              detail={"plan": p}, plan=plan_safe, t_ms=(perf_counter()-t0)*1000))
                    overall_ok = False

            except Exception as ex:
                results.append(err_result(action, "EXEC_ERROR", str(ex),
                                          detail={"plan": p}, plan=plan_safe, t_ms=(perf_counter()-t0)*1000))
                overall_ok = False

            try:
                dt = (perf_counter() - t0) * 1000.0
                if len(results) > before_len:
                    results[-1]["meta"]["time_ms"] = elapsed_ms + dt
                    elapsed_ms += dt
            except Exception:
                pass

        total_ms = (perf_counter() - t0_all) * 1000.0
        return {
            "ok": overall_ok,
            "schema": "bd2.v1",
            "results": results,
            "warnings": [],
            "stats": {"time_ms": total_ms}
        }