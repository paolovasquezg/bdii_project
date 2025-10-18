from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Union

Stmt = Union[dict, Any]

def _asdict(x: Stmt) -> dict:
    return asdict(x) if is_dataclass(x) else x

def _kind(d: dict) -> str:
    return d.get("kind","")

def _norm_type(b: str) -> str:
    b = (b or "").lower()
    if b in ("int","integer","smallint","bigint","serial"): return "int"
    if b in ("float","real"): return "float"
    if b in ("double","double precision"): return "double"
    if b in ("char","character"): return "char"
    if b in ("varchar","string","text"): return "varchar"
    if b in ("bool","boolean"): return "bool"
    if b in ("blob","binary"): return "blob"
    if b in ("date","datetime","timestamp"): return "date"
    return b or "varchar"

def _norm_method(m: Any) -> str | None:
    if m is None:
        return None
    s = str(m).strip().lower().replace(" ", "")
    # equivalencias comunes
    if s in {"b+", "bplus", "b+tree", "btree"}: return "bplus"
    if s in {"r-tree", "rtree", "r+tree", "rtree"}: return "rtree"
    if s in {"seq", "sequential"}: return "sequential"
    if s in {"isam"}: return "isam"
    if s in {"heap"}: return "heap"
    if s in {"hash", "hashing"}: return "hash"
    return s  # por si hay otros métodos válidos en tu backend

def _is_between(node: Any) -> bool:
    return isinstance(node, dict) and {"ident","lo","hi"} <= set(node.keys())

def _is_eq(node: Any) -> bool:
    return isinstance(node, dict) and node.get("op") in ("=","==") and {"left","right"} <= set(node.keys())

class Planner:
    def plan(self, stmts: List[Stmt]) -> List[Dict[str, Any]]:
        plans: List[Dict[str, Any]] = []
        for s in stmts:
            d = _asdict(s)
            k = _kind(d)

            # ----------------- CREATE TABLE -----------------
            if k == "create_table":
                fields = []
                # índices a nivel de tabla: [(col, method)]
                idx_tbl = {c: _norm_method(m) for (c, m) in d.get("table_indexes", [])}

                for col in d["columns"]:
                    f = {"name": col["name"], "type": _norm_type(col["type"]["base"])}
                    if col["type"]["length"] is not None:
                        f["length"] = int(col["type"]["length"])

                    # PK + posible USING
                    if col.get("primary_key"):
                        f["key"] = "primary"
                        if col.get("pk_using"):
                            f["index"] = _norm_method(col["pk_using"])

                    # índice inline en la columna
                    if col.get("inline_index") and "index" not in f:
                        f["index"] = _norm_method(col["inline_index"])

                    # índice definido a nivel de tabla
                    if col["name"] in idx_tbl and "index" not in f:
                        f["index"] = idx_tbl[col["name"]]

                    fields.append(f)

                plans.append({"action": "create_table", "table": d["name"], "fields": fields})

            # ----------------- CREATE INDEX -----------------
            elif k == "create_index":
                plans.append({
                    "action": "create_index",
                    "table": d["table"],
                    "column": d["column"],
                    "method": _norm_method(d.get("method") or "bplus"),
                    "if_not_exists": d.get("if_not_exists", False)
                })

            # --------- CREATE TABLE FROM FILE (opcional) ----------
            elif k == "create_table_from_file":
                plans.append({
                    "action": "create_table_from_file",
                    "table": d["name"],
                    "path": d["path"],
                    "if_not_exists": d.get("if_not_exists", False),
                    "index_method": _norm_method(d.get("index_method")),
                    "index_column": d.get("index_column")
                })

            # ----------------- INSERT -----------------
            elif k == "insert":
                cols = d.get("columns")

                # INSERT FROM FILE (si lo usas más arriba)
                if d.get("from_file"):
                    # No generamos plan especial aquí; tu executor ya soporta import desde CSV
                    pass

                # Normaliza a lista de filas
                rows = []
                if isinstance(d.get("rows"), list) and d["rows"]:
                    rows = [list(r) for r in d["rows"]]
                else:
                    vals = d.get("values")
                    if vals is None:
                        rows = []
                    elif isinstance(vals, list) and vals and isinstance(vals[0], (list, tuple)):
                        rows = [list(v) for v in vals]
                    else:
                        rows = [vals]

                for row in rows:
                    if cols is None:
                        plans.append({
                            "action": "insert",
                            "table": d["table"],
                            "record": row,
                            "record_is_positional": True
                        })
                    else:
                        plans.append({
                            "action": "insert",
                            "table": d["table"],
                            "record": {c: v for c, v in zip(cols, row)}
                        })

            # ----------------- SELECT -----------------
            elif k == "select":
                table = d["table"]
                where = d.get("where")
                cols = d.get("columns")  # None => *

                # sin WHERE -> select genérico
                if where is None:
                    plans.append({
                        "action": "select",
                        "table": table,
                        "columns": cols,
                        "where": None})
                    continue

                # WHERE como dict? (nuestro parser deja dataclasses->dict)
                if isinstance(where, dict):
                    # 1) BETWEEN
                    if _is_between(where):
                        plans.append({
                            "action": "range_search",
                            "table": table,
                            "field": where["ident"],
                            "min": where["lo"],
                            "max": where["hi"]
                        })

                    # 2) Igualdad (= o ==)
                    elif _is_eq(where):
                        plans.append({
                            "action": "search",
                            "table": table,
                            "field": where["left"],
                            "value": where["right"]
                        })

                    # 3) IN lista
                    elif "ident" in where and "items" in where:
                        plans.append({
                            "action": "search_in",
                            "table": table,
                            "field": where["ident"],
                            "items": where["items"]
                        })

                    # 4) GeoWithin (POINT, r)
                    elif {"ident","center","radius"} <= set(where.keys()):
                        center = where["center"]
                        if isinstance(center, dict) and center.get("kind") == "point":
                            plans.append({
                                "action": "geo_within",
                                "table": table,
                                "field": where["ident"],
                                "center": {"x": center["x"], "y": center["y"]},
                                "radius": where["radius"]
                            })
                        else:
                            # Si el centro no es POINT, dejamos que el executor filtre genérico
                            plans.append({"action": "select", "table": table, "columns": cols, "where": where})

                    elif {"ident", "point", "k"} <= set(where.keys()):
                        center = where["point"]
                        if isinstance(center, dict) and center.get("kind") == "point":
                            plans.append({
                                "action": "knn",
                                "table": table,
                                "field": where["ident"],
                                "point": (center["x"], center["y"]),
                                "k": int(where["k"])
                            })
                        else:
                            # fallback genérico (no debería ocurrir si parseamos POINT)
                            plans.append({"action": "select", "table": table, "columns": cols, "where": where})

                    # 5) AND entre (BETWEEN) y (=) en cualquier orden -> range + post_filter
                    elif where.get("op") == "AND" and isinstance(where.get("items"), list) and len(where["items"]) == 2:
                        a, b = where["items"]
                        # normaliza orden
                        left_between, right_eq = (a, b) if _is_between(a) and _is_eq(b) else ((b, a) if _is_between(b) and _is_eq(a) else (None, None))
                        if left_between and right_eq:
                            plans.append({
                                "action": "range_search",
                                "table": table,
                                "field": left_between["ident"],
                                "min": left_between["lo"],
                                "max": left_between["hi"],
                                "post_filter": {"field": right_eq["left"], "value": right_eq["right"]}
                            })
                        else:
                            # AND general -> select y que el executor filtre
                            plans.append({"action": "select", "table": table, "columns": cols, "where": where})

                    else:
                        # Forma no optimizada: select genérico (el executor filtra)
                        plans.append({"action": "select", "table": table, "columns": cols, "where": where})

                else:
                    # WHERE no-dict (por si viniera raro) -> select genérico
                    plans.append({"action": "select", "table": table, "columns": cols, "where": where})

            # ----------------- DELETE -----------------
            elif k == "delete":
                w = d.get("where") or {}
                if not (_is_eq(w)):
                    raise NotImplementedError("DELETE soporta igualdad simple (WHERE col = valor)")
                plans.append({"action": "remove", "table": d["table"], "field": w["left"], "value": w["right"]})

            # ----------------- DROP -----------------
            elif k == "drop_table":
                plans.append({"action": "drop_table", "table": d["name"], "if_exists": d.get("if_exists", False)})

            elif k == "drop_index":
                plans.append({
                    "action": "drop_index",
                    "table": d.get("table"),
                    "column": d.get("column"),
                    "name": d.get("name"),
                    "if_exists": d.get("if_exists", False)
                })

            else:
                raise NotImplementedError(f"No soportado en planner: {k}")

        return plans