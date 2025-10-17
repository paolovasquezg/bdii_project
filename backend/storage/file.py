from backend.catalog.catalog import get_json, get_filename
from backend.storage.indexes.heap import HeapFile
from backend.storage.indexes.sequential import SeqFile
from backend.storage.indexes.isam import IsamFile
from backend.storage.indexes.rtree import RTree
from backend.storage.indexes.hash import ExtendibleHashingFile
from backend.storage.indexes.bplus import BPlusFile
import json as _json
import struct
import csv
import os
import copy

DEBUG_IDX = os.getenv("BD2_DEBUG_INDEX", "0").lower() in ("1", "true", "yes")
DEBUG_IDX = 1

class File:
    # ------------------------------ helpers de catálogo ------------------------------ #

    def get_pk(self):
        for field in self.relation:
            if "key" in self.relation[field] and self.relation[field]["key"] == "primary":
                return field

    def __init__(self, table: str):
        self.filename = get_filename(table)
        self.relation, self.indexes = get_json(self.filename, 2)

        # compat viejos
        if isinstance(self.indexes, dict) and "primary" not in self.indexes and "indexes" in self.indexes:
            self.indexes["primary"] = self.indexes.pop("indexes")
        for col, spec in self.relation.items():
            if isinstance(spec, dict) and spec.get("key") == "indexes":
                spec["key"] = "primary"

        self.primary_key = self.get_pk()
        self.table = table

        self._io = self._new_io()
        self.last_io = self._new_io()
        self._index_usage = []

    # ------------------------------ IO accounting ------------------------------------ #

    def _new_io(self):
        zero = {"read_count": 0, "write_count": 0}
        return {
            "heap": dict(zero),
            "sequential": dict(zero),
            "isam": dict(zero),
            "bplus": dict(zero),
            "hash": dict(zero),
            "rtree": dict(zero),
            "total": dict(zero),
        }

    def io_reset(self):
        self._io = self._new_io()
        self.last_io = self._new_io()

    def io_merge(self, obj, kind: str):
        if obj is None: return
        rc = int(getattr(obj, "read_count", 0) or 0)
        wc = int(getattr(obj, "write_count", 0) or 0)
        if kind not in self._io: return
        self._io[kind]["read_count"] += rc
        self._io[kind]["write_count"] += wc
        self._io["total"]["read_count"] += rc
        self._io["total"]["write_count"] += wc

    def io_get(self):
        return copy.deepcopy(self._io)

    # ------------------------------ Index usage tracking ----------------------------- #

    def index_reset(self):
        self._index_usage = []

    def index_log(self, where: str, index_kind: str, field: str, op: str, note: str | None = None):
        try:
            self._index_usage.append({
                "where": str(where),
                "index": str(index_kind or ""),
                "field": str(field or ""),
                "op": str(op or ""),
                **({"note": str(note)} if note else {})
            })
        except Exception:
            pass

    def index_get(self):
        return copy.deepcopy(self._index_usage)

    # ------------------------------ helpers de tipos/rtree --------------------------- #

    def _coerce_types(self, rec: dict) -> dict:
        out = dict(rec)
        for col, spec in self.relation.items():
            if col not in out: continue
            v = out[col]
            t = (spec.get("type") or "").lower()
            if v is None or v == "":
                out[col] = None
            elif t in ("int", "integer", "i"):
                out[col] = int(v)
            elif t in ("float", "real", "double", "f"):
                out[col] = float(v)
            elif t in ("bool", "boolean"):
                out[col] = bool(v)
        return out

    def _posify(self, items):
        out = []
        for it in (items or []):
            if isinstance(it, int):
                out.append({"pos": it})
            elif isinstance(it, dict) and "pos" in it and isinstance(it["pos"], int):
                out.append({"pos": it["pos"]})
        return out

    def _primary(self):
        prim = self.indexes.get("primary") or self.indexes.get("indexes") or {}
        return prim.get("index"), prim.get("filename")

    def _make_rtree(self, field: str, *, heap_ok: bool):
        idx_meta = self.indexes[field]
        idx_filename = idx_meta["filename"]
        data_dir = os.path.dirname(os.path.dirname(idx_filename))
        return RTree(
            self.table, field, data_dir,
            key=field,
            M=int(idx_meta.get("M", 32)),
            heap_file=(self.indexes["primary"]["filename"] if heap_ok else None)
        )

    def _as_point(self, v):
        import json
        if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
            try: v = json.loads(v)
            except Exception: return False, None
        if isinstance(v, (list, tuple)) and len(v) >= 2 \
           and isinstance(v[0], (int, float)) and isinstance(v[1], (int, float)):
            return True, [float(v[0]), float(v[1])]
        return False, None

    def _bridge_from_rtree(self, items):
        if not items: return []
        is_heap = (self.indexes["primary"]["index"] == "heap")
        if is_heap:
            sample = items[0]
            if isinstance(sample, dict) and any(k in sample for k in self.relation.keys()):
                return items
            pos_list = []
            for it in items:
                if isinstance(it, int):
                    pos_list.append({"pos": it})
                elif isinstance(it, dict) and isinstance(it.get("pos"), int):
                    pos_list.append({"pos": it["pos"]})
            if not pos_list: return []
            hf = HeapFile(self.indexes["primary"]["filename"])
            out = hf.search_by_pos(pos_list)
            self.io_merge(hf, "heap")
            self.index_log("primary", "heap", self.primary_key, "search_by_pos")
            return out

        out = []
        for it in items:
            if isinstance(it, dict) and "pos" in it:
                pk = it["pos"]
                out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
        return out

    def _usable_secondary_kind(self, field: str):
        meta = self.indexes.get(field)
        if not meta: return None
        kind = (meta.get("index") or "").lower()
        return kind if kind in ("hash", "bplus", "rtree") else None

    # ----------------------------------- DDL build ----------------------------------- #

    def build(self, params):
        if self.indexes["primary"]["index"] != "isam":
            for record in params["records"]:
                self.insert({"op": "insert", "record": record})
            self.last_io = self.io_get()
            return

        mainfilename = self.indexes["primary"]["filename"]
        additional = {"key": None, "unique": []}

        for index in self.indexes:
            if self.indexes[index]["filename"] == mainfilename and index != "primary":
                additional["key"] = index
                break

        for field in self.relation:
            if "key" in self.relation[field] and self.relation[field]["key"] in ("primary", "unique"):
                additional["unique"].append(field)

        BuildFile = IsamFile(mainfilename)
        records = BuildFile.build(params["records"], additional)
        self.io_merge(BuildFile, "isam")
        self.index_log("primary", "isam", self.primary_key, "build")

        for index in self.indexes:
            if index == "primary" or self.indexes[index]["filename"] == mainfilename:
                continue

            filename = self.indexes[index]["filename"]
            kind = self.indexes[index]["index"]

            if kind == "hash":
                try:
                    h = ExtendibleHashingFile(filename)
                    for rec in records:
                        if index not in rec: continue
                        in_rec = ({"pos": rec.get("pos"), index: rec[index], "deleted": False}
                                  if self.indexes["primary"]["index"] == "heap"
                                  else {"pk": rec[self.primary_key], index: rec[index], "deleted": False})
                        h.insert(in_rec, key_name=index)
                    self.io_merge(h, "hash")
                    self.index_log("secondary", "hash", index, "build")
                except Exception as e:
                    if DEBUG_IDX: print("[HASH build secondary] skip:", e)

            elif kind == "bplus":
                try:
                    bp = BPlusFile(filename)
                    for rec in records:
                        if index not in rec: continue
                        in_rec = ({index: rec[index], "pos": rec.get("pos"), "deleted": False}
                                  if self.indexes["primary"]["index"] == "heap"
                                  else {index: rec[index], "pk": rec[self.primary_key], "deleted": False})
                        bp.insert(in_rec, {"key": index})
                    self.io_merge(bp, "bplus")
                    self.index_log("secondary", "bplus", index, "build")
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS build secondary] skip:", e)

            elif kind == "rtree":
                try:
                    rt = self._make_rtree(index, heap_ok=False)
                    for rec in records:
                        if index not in rec: continue
                        ok, pt = self._as_point(rec[index])
                        if not ok: continue
                        in_rec = ({"pos": rec.get("pos"), index: pt, "deleted": False}
                                  if self.indexes["primary"]["index"] == "heap"
                                  else {"pk": rec[self.primary_key], index: pt, "deleted": False})
                        rt.insert(in_rec)
                    self.io_merge(rt, "rtree")
                    self.index_log("secondary", "rtree", index, "build")
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE build secondary] skip:", e)

        self.last_io = self.io_get()
        return []

    # ----------------------------------- DML insert ---------------------------------- #

    def insert(self, params):
        mainfilename = self.indexes["primary"]["filename"]

        record = self._coerce_types(params["record"])
        record["deleted"] = False

        # -------- PRE-CHEQUEO DE DUPLICADOS (PK/UNIQUE) --------
        try:
            unique_fields = []
            for col, spec in self.relation.items():
                if isinstance(spec, dict) and (spec.get("key") in ("primary", "unique")):
                    unique_fields.append(col)
            for u in unique_fields:
                if u not in record: continue
                existing = self.search({"op": "search", "field": u, "value": record[u]}) or []
                if existing:
                    # No insertar nada: el executor reporta DUPLICATE_KEY
                    self.index_log("precheck", "meta", u, "duplicate")
                    self.last_io = self.io_get()
                    return []
        except Exception:
            # si algo falla aquí, seguimos y dejamos que el primario lo resuelva
            pass

        additional = {"key": None, "unique": []}
        for index in self.indexes:
            if self.indexes[index]["filename"] == mainfilename and index != "primary":
                additional["key"] = index
                break
        for field in self.relation:
            if "key" in self.relation[field] and self.relation[field]["key"] in ("primary", "unique"):
                additional["unique"].append(field)

        maindex = self.indexes["primary"]["index"]
        if maindex == "heap":
            hf = HeapFile(mainfilename)
            records = hf.insert(record, additional)              # [(row_dict, pos), ...]
            self.io_merge(hf, "heap")
            self.index_log("primary", "heap", self.primary_key, "insert")

        elif maindex == "sequential":
            sf = SeqFile(mainfilename)
            records = sf.insert(record, additional)              # [(row_dict, pos), ...] o [row_dict]
            self.io_merge(sf, "sequential")
            self.index_log("primary", "sequential", self.primary_key, "insert")

        elif maindex == "isam":
            isf = IsamFile(mainfilename)
            # build@first_insert si sólo está el header
            try:
                with open(mainfilename, "rb") as f:
                    slen = struct.unpack("<I", f.read(4))[0]
                    f.seek(0, 2)
                    end = f.tell()
                    data_start = 4 + slen
            except Exception:
                end = None; data_start = None

            if end is not None and data_start is not None and end <= data_start:
                records = isf.build([record], additional)        # [row_dict]
                self.index_log("primary", "isam", self.primary_key, "build@first_insert")
            else:
                records = isf.insert(record, additional)         # [row_dict]
                self.index_log("primary", "isam", self.primary_key, "insert")
            self.io_merge(isf, "isam")

        elif maindex == "bplus":
            bp = BPlusFile(mainfilename)
            bp.insert(record, {"key": self.primary_key})
            records = [record]                                   # [row_dict]
            self.io_merge(bp, "bplus")
            self.index_log("primary", "bplus", self.primary_key, "insert")

        else:
            records = []

        if len(records) >= 1:
            is_heap = (self.indexes["primary"]["index"] == "heap")

            for index in self.indexes:
                if index == "primary" or self.indexes[index]["filename"] == mainfilename:
                    continue

                filename = self.indexes[index]["filename"]
                kind = self.indexes[index]["index"]

                if kind == "hash":
                    try:
                        h = ExtendibleHashingFile(filename)
                        if is_heap:
                            for row_dict, pos in records:
                                if index not in row_dict: continue
                                rec = {"pos": pos, index: row_dict[index], "deleted": False}
                                h.insert(rec, key_name=index)
                        else:
                            for row_dict in records:
                                if index not in row_dict: continue
                                rec = {"pk": row_dict[self.primary_key], index: row_dict[index], "deleted": False}
                                h.insert(rec, key_name=index)
                        self.io_merge(h, "hash")
                        self.index_log("secondary", "hash", index, "insert")
                    except Exception as e:
                        if DEBUG_IDX: print("[HASH insert secondary] skip:", e)

                elif kind == "bplus":
                    try:
                        bp = BPlusFile(filename)
                        if is_heap:
                            for row_dict, pos in records:
                                if index not in row_dict: continue
                                rec = {"pos": pos, index: row_dict[index], "deleted": False}
                                bp.insert(rec, {"key": index})
                        else:
                            for row_dict in records:
                                if index not in row_dict: continue
                                rec = {index: row_dict[index], "pk": row_dict[self.primary_key], "deleted": False}
                                bp.insert(rec, {"key": index})
                        self.io_merge(bp, "bplus")
                        self.index_log("secondary", "bplus", index, "insert")
                    except Exception as e:
                        if DEBUG_IDX: print("[BPLUS insert secondary] skip:", e)

                elif kind == "rtree":
                    try:
                        rt = self._make_rtree(index, heap_ok=is_heap)
                        if is_heap:
                            for row_dict, pos in records:
                                if index not in row_dict: continue
                                ok, pt = self._as_point(row_dict[index])
                                if not ok: continue
                                in_rec = {"pos": pos, index: pt, "deleted": False}
                                rt.insert(in_rec)
                        else:
                            for row_dict in records:
                                if index not in row_dict: continue
                                ok, pt = self._as_point(row_dict[index])
                                if not ok: continue
                                in_rec = {"pk": row_dict[self.primary_key], index: pt, "deleted": False}
                                rt.insert(in_rec)
                        self.io_merge(rt, "rtree")
                        self.index_log("secondary", "rtree", index, "insert")
                    except Exception as e:
                        if DEBUG_IDX: print("[RTREE insert secondary] skip:", e)

        self.last_io = self.io_get()
        return records

    # ----------------------------------- DML search ---------------------------------- #

    def search(self, params: dict):
        field = params["field"]
        value = params["value"]
        records = []

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]

        if "key" in self.relation[field]:
            if self.relation[field]["key"] in ("primary", "unique"):
                additional["unique"] = True

        same_key = (field == self.primary_key)
        sec_kind = self._usable_secondary_kind(field)
        use_primary = (sec_kind is None)

        if use_primary:
            if mainindx == "heap":
                hf = HeapFile(mainfilename)
                records = hf.search(additional)
                self.io_merge(hf, "heap")
                self.index_log("primary", "heap", field, "search", note="same_key")

            elif mainindx == "sequential":
                sf = SeqFile(mainfilename)
                records = sf.search(additional, same_key)
                self.io_merge(sf, "sequential")
                self.index_log("primary", "sequential", field, "search", note="same_key")

            elif mainindx == "isam":
                isf = IsamFile(mainfilename)
                records = isf.search(additional, same_key)
                self.io_merge(isf, "isam")
                self.index_log("primary", "isam", field, "search", note="same_key")

            elif mainindx == "bplus":
                try:
                    bp = BPlusFile(mainfilename)
                    records = bp.search(additional, same_key)
                    self.index_log("primary", "bplus", field, "search")
                    self.io_merge(bp, "bplus")
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS search primary] skip:", e)
                    records = []

            else:
                records = []

        else:
            filename = self.indexes[field]["filename"]
            kind = sec_kind

            if kind == "hash":
                try:
                    h = ExtendibleHashingFile(filename)
                    records = h.find(value, key_name=field)
                    self.io_merge(h, "hash")
                    self.index_log("secondary", "hash", field, "search")
                except Exception as e:
                    if DEBUG_IDX: print("[HASH search secondary] skip:", e)
                    records = []

            elif kind == "bplus":
                try:
                    bp = BPlusFile(filename)
                    records = bp.search(additional, same_key=True)
                    self.io_merge(bp, "bplus")
                    self.index_log("secondary", "bplus", field, "search")
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS search secondary] skip:", e)
                    records = []

            elif kind == "rtree":
                try:
                    is_heap = (self.indexes["primary"]["index"] == "heap")
                    rt = self._make_rtree(field, heap_ok=is_heap)
                    EPS = 1e-9
                    x = y = None
                    ok_pt, pt = self._as_point(value)
                    if ok_pt:
                        x, y = pt[0], pt[1]
                    elif isinstance(value, dict) and {"x", "y"} <= set(value.keys()):
                        x, y = float(value["x"]), float(value["y"])
                    else:
                        try:
                            x = float(value); y = 0.0
                        except Exception:
                            raise ValueError("RTREE equality needs a point-like value")
                    items = rt.search_rect(x - EPS, x + EPS, y - EPS, y + EPS)
                    self.io_merge(rt, "rtree")
                    self.index_log("secondary", "rtree", field, "search_rect_eq")
                    self.last_io = self.io_get()
                    return self._bridge_from_rtree(items)
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE search secondary] skip:", e)
                    self.last_io = self.io_get()
                    return []

            if self.indexes["primary"]["index"] == "heap":
                hf = HeapFile(mainfilename)
                out = hf.search_by_pos(self._posify(records))
                self.io_merge(hf, "heap")
                self.index_log("primary", "heap", self.primary_key, "search_by_pos")
                self.last_io = self.io_get()
                return out

            ret_records = []
            for rec in records:
                ret_records.extend(
                    self.search({"op": "search", "field": self.primary_key, "value": rec["pk"]})
                )
            records = ret_records

        self.last_io = self.io_get()
        return records

    # ------------------------------- DML range_search ------------------------------- #

    def range_search(self, params: dict):
        field = params["field"]
        additional = {"key": field}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        records = []

        same_key = (field == self.primary_key)
        sec_kind = self._usable_secondary_kind(field)
        use_primary = (sec_kind is None)

        if use_primary:
            if mainindx == "heap":
                additional["min"] = params["min"]; additional["max"] = params["max"]
                hf = HeapFile(mainfilename)
                records = hf.range_search(additional)
                self.io_merge(hf, "heap")
                self.index_log("primary", "heap", field, "range_search", note="same_key" )

            elif mainindx == "sequential":
                additional["min"] = params["min"]; additional["max"] = params["max"]
                sf = SeqFile(mainfilename)
                records = sf.range_search(additional, same_key)
                self.io_merge(sf, "sequential")
                self.index_log("primary", "sequential", field, "range_search", note="same_key")

            elif mainindx == "isam":
                additional["min"] = params["min"]; additional["max"] = params["max"]
                isf = IsamFile(mainfilename)
                records = isf.range_search(additional, same_key)
                self.io_merge(isf, "isam")
                self.index_log("primary", "isam", field, "range_search", note="same_key" )

            elif mainindx == "bplus":
                try:
                    bp = BPlusFile(mainfilename)
                    additional["min"] = params["min"]
                    additional["max"] = params["max"]
                    records = bp.range_search(additional, same_key=True)
                    self.index_log("primary", "bplus", field, "range_search")
                    self.io_merge(bp, "bplus")
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS range_search primary] skip:", e)
                    records = []

            else:
                records = []

        else:
            filename = self.indexes[field]["filename"]
            kind = sec_kind

            if kind == "bplus":
                try:
                    bp = BPlusFile(filename)
                    records = bp.range_search({"key": field, "min": params["min"], "max": params["max"]}, same_key=True)
                    self.io_merge(bp, "bplus")
                    self.index_log("secondary", "bplus", field, "range_search")
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS range_search secondary] skip:", e)
                    records = []

            elif kind == "rtree":
                try:
                    rect = params.get("rect") or {
                        "xmin": params["min"], "xmax": params["max"],
                        "ymin": params.get("ymin", params["min"]),
                        "ymax": params.get("ymax", params["max"]),
                    }
                    is_heap = (self.indexes["primary"]["index"] == "heap")
                    rt = self._make_rtree(field, heap_ok=is_heap)
                    items = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])
                    self.io_merge(rt, "rtree")
                    self.index_log("secondary", "rtree", field, "range_rect")
                    out = self._bridge_from_rtree(items)
                    self.last_io = self.io_get()
                    records = out
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE range_search secondary] skip:", e)
                    return []

            if self.indexes["primary"]["index"] == "heap":
                hf = HeapFile(mainfilename)
                out = hf.search_by_pos(self._posify(records))
                self.io_merge(hf, "heap")
                self.index_log("primary", "heap", self.primary_key, "search_by_pos")
                self.last_io = self.io_get()
                return out

            ret_records = []
            for rec in records:
                ret_records.extend(
                    self.search({"op": "search", "field": self.primary_key, "value": rec["pk"]})
                )
            records = ret_records

        self.last_io = self.io_get()
        return records

    # ----------------------------------- DML knn ------------------------------------ #

    def knn(self, params: dict):
        field = params["field"]
        if field not in self.indexes:
            self.last_io = self.io_get(); return []
        if "key" in self.relation.get(field, {}) and self.relation[field]["key"] == "primary":
            self.last_io = self.io_get(); return []
        if self.indexes[field]["index"] != "rtree":
            self.last_io = self.io_get(); return []
        try:
            is_heap = (self.indexes["primary"]["index"] == "heap")
            rt = self._make_rtree(field, heap_ok=is_heap)
            items = rt.knn(params["point"][0], params["point"][1], params["k"])
            self.io_merge(rt, "rtree")
            self.index_log("secondary", "rtree", field, "knn")
            out = self._bridge_from_rtree(items)
            self.last_io = self.io_get()
            return out
        except Exception as e:
            if DEBUG_IDX: print("[RTREE knn] skip:", e)
            self.last_io = self.io_get()
            return []

    # ----------------------------------- DML remove --------------------------------- #

    def remove(self, params):
        field = params["field"]
        value = params["value"]

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        records = []

        if "key" in self.relation.get(field, {}):
            if self.relation[field]["key"] in ("primary", "unique"):
                additional["unique"] = True

        same_key = (field == self.primary_key)
        sec_kind = self._usable_secondary_kind(field)

        if mainindx == "heap":
            hf = HeapFile(mainfilename)
            records = hf.remove(additional)  # [(row_dict, pos), ...]
            self.io_merge(hf, "heap")
            self.index_log("primary", "heap", field, "remove", note="same_key")

        elif mainindx == "sequential":
            sf = SeqFile(mainfilename)
            records = sf.remove(additional, same_key)
            self.io_merge(sf, "sequential")
            self.index_log("primary", "sequential", field, "remove", note="same_key")

        elif mainindx == "isam":
            isf = IsamFile(mainfilename)
            records = isf.remove(additional, same_key)
            self.io_merge(isf, "isam")
            self.index_log("primary", "isam", field, "remove", note="same_key")

        elif mainindx == "bplus":
            try:
                bp = BPlusFile(mainfilename)
                records = bp.remove(additional, same_key)
                self.index_log("primary", "bplus", field, "remove")
                self.io_merge(bp, "bplus")
            except Exception as e:
                if DEBUG_IDX: print("[BPLUS remove primary] skip:", e)
                records = []

        # limpiar secundarios
        for index in self.indexes:
            if index == "primary" or self.indexes[index]["filename"] == mainfilename:
                continue
            kind = self.indexes[index]["index"]
            filename = self.indexes[index]["filename"]

            if kind == "hash":
                try:
                    h = ExtendibleHashingFile(filename)
                    for rec in (records or []):
                        if isinstance(rec, tuple) and len(rec) >= 2:
                            row = rec[0]
                            if index in row:
                                try: h.remove(row[index], key_name=index)
                                except Exception: pass
                        elif isinstance(rec, dict) and index in rec:
                            try: h.remove(rec[index], key_name=index)
                            except Exception: pass
                    self.io_merge(h, "hash")
                    self.index_log("secondary", "hash", index, "cleanup_after_remove")
                except Exception as e:
                    if DEBUG_IDX: print("[HASH remove secondary] skip:", e)

            elif kind == "bplus":
                try:
                    bp = BPlusFile(filename)
                    for rec in (records or []):
                        if isinstance(rec, tuple) and len(rec) >= 2:
                            row = rec[0]
                            if index in row:
                                try: bp.remove({"key": index, "value": row[index]})
                                except Exception: pass
                        elif isinstance(rec, dict) and index in rec:
                            try: bp.remove({"key": index, "value": rec[index]})
                            except Exception: pass
                    self.io_merge(bp, "bplus")
                    self.index_log("secondary", "bplus", index, "cleanup_after_remove")
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS remove secondary] skip:", e)

            elif kind == "rtree":
                try:
                    is_heap = (mainindx == "heap")
                    rt = self._make_rtree(index, heap_ok=is_heap)
                    for rec in (records or []):
                        if isinstance(rec, tuple) and len(rec) >= 2:
                            row, pos = rec[0], rec[1]
                            ok, pt = self._as_point(row.get(index))
                            if not ok: continue
                            try: rt.remove({"pos": pos, index: pt})
                            except Exception: pass
                        elif isinstance(rec, dict):
                            pos = rec.get("pos", rec.get(self.primary_key))
                            ok, pt = self._as_point(rec.get(index))
                            if pos is None or not ok: continue
                            try: rt.remove({"pos": pos, index: pt})
                            except Exception: pass
                    self.io_merge(rt, "rtree")
                    self.index_log("secondary", "rtree", index, "cleanup_after_remove")
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE remove secondary] skip:", e)

        self.last_io = self.io_get()
        return records

    # ----------------------------------- execute ------------------------------------ #

    def execute(self, params: dict):
        if params["op"] == "build":
            return self.build(params)
        elif params["op"] == "insert":
            return self.insert(params)
        elif params["op"] == "search":
            return self.search(params)
        elif params["op"] == "range_search":
            return self.range_search(params)
        elif params["op"] == "knn":
            return self.knn(params)
        elif params["op"] == "remove":
            return self.remove(params)
        elif params["op"] == "rtree_within_circle":
            try:
                field = params["field"]
                cx, cy = float(params["center"]["x"]), float(params["center"]["y"])
                rr = float(params["radius"])
                is_heap = (self.indexes["primary"]["index"] == "heap")
                rt = self._make_rtree(field, heap_ok=is_heap)
                items = rt.range(cx, cy, rr)
                self.io_merge(rt, "rtree")
                self.index_log("secondary", "rtree", field, "rtree_within_circle")
                out = self._bridge_from_rtree(items)
                self.last_io = self.io_get()
                return out
            except Exception as e:
                if DEBUG_IDX: print("[RTREE within_circle] skip:", e)
                return []

        elif params["op"] == "rtree_range":
            try:
                field = params["field"]
                rect = params["rect"]
                is_heap = (self.indexes["primary"]["index"] == "heap")
                rt = self._make_rtree(field, heap_ok=is_heap)
                items = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])
                self.io_merge(rt, "rtree")
                self.index_log("secondary", "rtree", field, "rtree_range")
                out = self._bridge_from_rtree(items)
                self.last_io = self.io_get()
                return out
            except Exception as e:
                if DEBUG_IDX: print("[RTREE range op] skip:", e)
                self.last_io = self.io_get()
                return []
        elif params["op"] == "import_csv":
            path = params["path"]
            inserted = 0
            with open(path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    rec = {}
                    for col, spec in self.relation.items():
                        if col not in row: continue
                        v = row[col]
                        if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                            try: v = _json.loads(v)
                            except Exception: pass
                        t = (spec.get("type") or "").lower()
                        if v == "" or v is None:
                            v = None
                        elif t in ("int", "i"):
                            v = int(v)
                        elif t in ("float", "real", "f", "double"):
                            v = float(v)
                        rec[col] = v
                    self.insert({"record": rec})
                    inserted += 1
            self.last_io = self.io_get()
            return {"count": inserted}
