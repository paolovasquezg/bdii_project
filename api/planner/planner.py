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

class Planner:
    def plan(self, stmts: List[Stmt]) -> List[Dict[str, Any]]:
        plans: List[Dict[str, Any]] = []
        for s in stmts:
            d = _asdict(s)
            k = _kind(d)

            if k == "create_table":
                fields = []
                idx_tbl = {c: m for (c, m) in d.get("table_indexes", [])}
                for col in d["columns"]:
                    f = {"name": col["name"], "type": _norm_type(col["type"]["base"])}
                    if col["type"]["length"] is not None:
                        f["length"] = int(col["type"]["length"])
                    if col.get("primary_key"):
                        f["key"] = "primary"
                        if col.get("pk_using"):
                            f["index"] = str(col["pk_using"])
                    if col.get("inline_index") and "index" not in f:
                        f["index"] = str(col["inline_index"])
                    if col["name"] in idx_tbl and "index" not in f:
                        f["index"] = str(idx_tbl[col["name"]])
                    fields.append(f)
                plans.append({"action": "create_table", "table": d["name"], "fields": fields})

            elif k == "create_index":
                plans.append({
                    "action": "create_index",
                    "table": d["table"],
                    "column": d["column"],
                    "method": d.get("method") or "b+",
                    "if_not_exists": d.get("if_not_exists", False)
                })

            elif k == "create_table_from_file":
                plans.append({
                    "action": "create_table_from_file",
                    "table": d["name"],
                    "path": d["path"],
                    "if_not_exists": d.get("if_not_exists", False),
                    "index_method": d.get("index_method"),
                    "index_column": d.get("index_column")
                })

            elif k == "insert":
                cols = d.get("columns")
                vals = d.get("values")
                # Soportamos single-row INSERT
                if isinstance(vals, list) and (not vals or not isinstance(vals[0], (list, tuple))):
                    row = vals
                elif isinstance(vals, list) and vals and isinstance(vals[0], (list, tuple)):
                    row = list(vals[0])
                else:
                    row = [vals]
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

            elif k == "select":
                table = d["table"]
                where = d.get("where")
                if where is None:
                    raise NotImplementedError("SELECT sin WHERE no está planificado aún")

                # BETWEEN: {'ident','lo','hi'}
                if isinstance(where, dict) and {"ident","lo","hi"} <= set(where.keys()):
                    plans.append({
                        "action": "range search",
                        "table": table,
                        "field": where["ident"],
                        "min": where["lo"],
                        "max": where["hi"]
                    })

                # Igualdad
                elif isinstance(where, dict) and where.get("op") == "=":
                    plans.append({
                        "action": "search",
                        "table": table,
                        "field": where["left"],
                        "value": where["right"]
                    })

                # IN lista: {'ident', 'items'}
                elif isinstance(where, dict) and "ident" in where and "items" in where:
                    plans.append({
                        "action": "search_in",
                        "table": table,
                        "field": where["ident"],
                        "items": where["items"]
                    })

                # GeoWithin: {'ident','center':{'kind':'point','x','y'}, 'radius'}
                elif isinstance(where, dict) and "center" in where and "radius" in where and "ident" in where:
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
                        raise NotImplementedError("GeoWithin requiere POINT(x,y) como centro")

                # BETWEEN AND igualdad (AND con 2 items) — preservo tu caso especial
                elif isinstance(where, dict) and where.get("op") == "AND" and len(where.get("items", [])) == 2:
                    a, b = where["items"]
                    if isinstance(a, dict) and {"ident","lo","hi"} <= set(a.keys()) and isinstance(b, dict) and b.get("op") == "=":
                        plans.append({
                            "action": "range search",
                            "table": table,
                            "field": a["ident"],
                            "min": a["lo"],
                            "max": a["hi"],
                            "post_filter": {"field": b["left"], "value": b["right"]}
                        })
                    else:
                        raise NotImplementedError("AND general no planificado")

                else:
                    raise NotImplementedError("WHERE no soportado")

            elif k == "delete":
                w = d.get("where") or {}
                if w.get("op") != "=":
                    raise NotImplementedError("DELETE soporta igualdad simple (WHERE id = x)")
                plans.append({"action": "remove", "table": d["table"], "field": w["left"], "value": w["right"]})

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
