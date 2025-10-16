from backend.catalog.catalog import get_json, get_filename
from backend.storage.indexes.heap import HeapFile
from backend.storage.indexes.sequential import SeqFile
from backend.storage.indexes.isam import IsamFile
from backend.storage.indexes.rtree import RTree
from backend.storage.indexes.hash import ExtendibleHashingFile
from backend.storage.indexes.bplus import BPlusFile
import json as _json
import struct
from backend.core.utils import build_format
from backend.core.record import Record
import os

DEBUG_IDX = os.getenv("BD2_DEBUG_INDEX", "0").lower() in ("1", "true", "yes")

class File:
    # ------------------------------ helpers de catálogo ------------------------------ #

    def get_pk(self):
        """Devuelve el nombre de la columna marcada como PRIMARY KEY en el schema."""
        for field in self.relation:
            if "key" in self.relation[field] and self.relation[field]["key"] == "primary":
                return field

    def __init__(self, table: str):
        """Carga metadatos (schema + índices) y normaliza catálogos viejos."""
        self.filename = get_filename(table)
        self.relation, self.indexes = get_json(self.filename, 2)

        # COMPAT: catálogos antiguos guardaron la PK bajo 'indexes'. Normalizamos a 'primary'.
        if isinstance(self.indexes, dict) and "primary" not in self.indexes and "indexes" in self.indexes:
            self.indexes["primary"] = self.indexes.pop("indexes")

        # COMPAT: algunos schemas marcaron key=='indexes' en la columna PK.
        for col, spec in self.relation.items():
            if isinstance(spec, dict) and spec.get("key") == "indexes":
                spec["key"] = "primary"

        self.primary_key = self.get_pk()
        self.table = table

    # ------------------------------ helpers de tipos/rtree ------------------------------ #

    def _coerce_types(self, rec: dict) -> dict:
        """Castea valores a los tipos declarados en el schema (int/float/bool)."""
        out = dict(rec)
        for col, spec in self.relation.items():
            if col not in out:
                continue
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
            # strings/varchar/otros se dejan tal cual
        return out

    def _posify(self, items):
        """Normaliza una lista de ints o dicts a [{'pos': int}, ...] para heap.search_by_pos/delete_by_pos."""
        out = []
        for it in (items or []):
            if isinstance(it, int):
                out.append({"pos": it})
            elif isinstance(it, dict) and "pos" in it and isinstance(it["pos"], int):
                out.append({"pos": it["pos"]})
        return out

    def _primary(self):
        """Devuelve (kind, filename) del índice primario (tolerante a catálogos viejos)."""
        prim = self.indexes.get("primary") or self.indexes.get("indexes") or {}
        return prim.get("index"), prim.get("filename")

    def _make_rtree(self, field: str, *, heap_ok: bool):
        """Fábrica de RTree asegurando que M sea int y seteando heap_file si aplica."""
        idx_meta = self.indexes[field]
        idx_filename = idx_meta["filename"]
        # data_dir = .../runtime/files   (padre de la carpeta <tabla>/)
        data_dir = os.path.dirname(os.path.dirname(idx_filename))
        return RTree(
            self.table, field, data_dir,
            key=field,
            M=int(idx_meta.get("M", 32)),  # cast seguro
            heap_file=(self.indexes["primary"]["filename"] if heap_ok else None)
        )

    def _as_point(self, v):
        """Valida/convierte 'v' en punto [x,y] (acepta string JSON '[x,y]')."""
        import json
        if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
            try:
                v = json.loads(v)
            except Exception:
                return False, None
        if isinstance(v, (list, tuple)) and len(v) >= 2 \
                and isinstance(v[0], (int, float)) and isinstance(v[1], (int, float)):
            return True, [float(v[0]), float(v[1])]
        return False, None

    def _bridge_from_rtree(self, items):
        """Traduce resultados del R-Tree a filas:
           - Si primario es HEAP: posiciones → HeapFile.search_by_pos; si ya son filas, devuelve tal cual.
           - Si primario NO es HEAP: {'pos': pk} → resolver por PK vía search().
        """
        if not items:
            return []

        is_heap = (self.indexes["primary"]["index"] == "heap")
        if is_heap:
            sample = items[0]
            # ¿Ya son filas completas?
            if isinstance(sample, dict) and any(k in sample for k in self.relation.keys()):
                return items
            # Normalizar a [{'pos': int}, ...] + resolver en heap
            pos_list = []
            for it in items:
                if isinstance(it, int):
                    pos_list.append({"pos": it})
                elif isinstance(it, dict) and isinstance(it.get("pos"), int):
                    pos_list.append({"pos": it["pos"]})
            if not pos_list:
                return []
            hf = HeapFile(self.indexes["primary"]["filename"])
            return hf.search_by_pos(pos_list)

        # Primario NO-HEAP: 'pos' representa PK → resolver por PK
        out = []
        for it in items:
            if isinstance(it, dict) and "pos" in it:
                pk = it["pos"]
                out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
        return out

    # ----------------------------------- DDL build ----------------------------------- #

    def build(self, params):
        """Construye datos iniciales. Si primario es ISAM, usa ruta especial; si no, reusa insert()."""
        if self.indexes["primary"]["index"] != "isam":
            # Para HEAP/SEQ/B+ → reusar insert por cada fila.
            for record in params["records"]:
                self.insert({"op": "insert", "record": record})
            return

        # Ruta especial: primario ISAM → construye base y luego secundarios
        mainfilename = self.indexes["primary"]["filename"]
        additional = {"key": None, "unique": []}

        # Detectar la columna clave para el ISAM si comparte archivo
        for index in self.indexes:
            if self.indexes[index]["filename"] == mainfilename and index != "primary":
                additional["key"] = index
                break

        # Uniques (PK y UNIQUE) para enforcement
        for field in self.relation:
            if "key" in self.relation[field] and self.relation[field]["key"] in ("primary", "unique"):
                additional["unique"].append(field)

        # Construir el archivo ISAM principal; 'records' queda como lista de dicts completos
        BuildFile = IsamFile(mainfilename)
        records = BuildFile.build(params["records"], additional)

        # Poblar índices secundarios (cada bloque protegido)
        for index in self.indexes:
            if index == "primary" or self.indexes[index]["filename"] == mainfilename:
                continue

            filename = self.indexes[index]["filename"]
            kind = self.indexes[index]["index"]

            if kind == "hash":
                try:
                    h = ExtendibleHashingFile(filename)
                    for rec in records:  # dicts completos
                        if index not in rec:
                            continue
                        h.insert({"pk": rec[self.primary_key], index: rec[index], "deleted": False},
                                 key_name=index)
                except Exception as e:
                    if DEBUG_IDX: print("[HASH build secondary] skip:", e)

            elif kind == "b+":
                try:
                    bp = BPlusFile(filename)
                    for rec in records:
                        if index not in rec:
                            continue
                        in_rec = {index: rec[index], "pk": rec[self.primary_key], "deleted": False}
                        bp.insert(in_rec, {"key": index})
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS build secondary] skip:", e)

            elif kind == "rtree":
                try:
                    rt = self._make_rtree(index, heap_ok=False)  # ISAM no es heap
                    for rec in records:
                        if index not in rec:
                            continue
                        ok, pt = self._as_point(rec[index])
                        if not ok:
                            continue
                        in_rec = {"pos": rec[self.primary_key], index: pt, "deleted": False}
                        rt.insert(in_rec)
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE build secondary] skip:", e)

    # ----------------------------------- DML insert ---------------------------------- #

    def insert(self, params):
        """Inserta una fila en el primario y actualiza secundarios (protegidos)."""
        mainfilename = self.indexes["primary"]["filename"]

        # 1) Sanitizar tipos primero y luego marcar 'deleted'
        record = self._coerce_types(params["record"])
        record["deleted"] = False

        # Uniques y clave del ISAM (si aplica)
        additional = {"key": None, "unique": []}
        for index in self.indexes:
            if self.indexes[index]["filename"] == mainfilename and index != "primary":
                additional["key"] = index
                break
        for field in self.relation:
            if "key" in self.relation[field] and self.relation[field]["key"] in ("primary", "unique"):
                additional["unique"].append(field)

        # 2) Insertar en PRIMARIO
        maindex = self.indexes["primary"]["index"]
        if maindex == "heap":
            records = HeapFile(mainfilename).insert(record, additional)              # → [(row_dict, pos), ...]
        elif maindex == "sequential":
            records = SeqFile(mainfilename).insert(record, additional)
        elif maindex == "isam":
            records = IsamFile(mainfilename).insert(record, additional)
        elif maindex == "b+":
            BPlusFile(mainfilename).insert(record, {"key": self.primary_key})
            records = [record]                                                       # → [row_dict]
        else:
            records = []

        # 3) Replicar en SECUNDARIOS (protegidos; si algo falla, no rompe el DML)
        if len(records) >= 1:
            for index in self.indexes:
                if index == "primary" or self.indexes[index]["filename"] == mainfilename:
                    continue

                filename = self.indexes[index]["filename"]
                kind = self.indexes[index]["index"]

                if kind == "hash":
                    try:
                        h = ExtendibleHashingFile(filename)
                        if self.indexes["primary"]["index"] == "heap":
                            # records: [(row_dict, pos), ...]
                            for row_dict, pos in records:
                                if index not in row_dict:
                                    continue
                                rec = {"pos": pos, index: row_dict[index], "deleted": False}
                                h.insert(rec, key_name=index)
                        else:
                            # records: [row_dict, ...]
                            for row_dict in records:
                                if index not in row_dict:
                                    continue
                                rec = {"pk": row_dict[self.primary_key], index: row_dict[index], "deleted": False}
                                h.insert(rec, key_name=index)
                    except Exception as e:
                        if DEBUG_IDX: print("[HASH insert secondary] skip:", e)

                elif kind == "b+":
                    try:
                        bp = BPlusFile(filename)
                        if self.indexes["primary"]["index"] == "heap":
                            for row_dict, pos in records:
                                if index not in row_dict:
                                    continue
                                rec = {"pos": pos, index: row_dict[index], "deleted": False}
                                bp.insert(rec, {"key": index})
                        else:
                            for row_dict in records:
                                if index not in row_dict:
                                    continue
                                rec = {index: row_dict[index], "pk": row_dict[self.primary_key], "deleted": False}
                                bp.insert(rec, {"key": index})
                    except Exception as e:
                        if DEBUG_IDX: print("[BPLUS insert secondary] skip:", e)

                elif kind == "rtree":
                    try:
                        is_heap = (self.indexes["primary"]["index"] == "heap")
                        rt = self._make_rtree(index, heap_ok=is_heap)
                        if is_heap:
                            # records: [(row_dict, pos), ...]
                            for row_dict, pos in records:
                                if index not in row_dict:
                                    continue
                                ok, pt = self._as_point(row_dict[index])
                                if not ok:
                                    continue
                                in_rec = {"pos": pos, index: pt, "deleted": False}
                                rt.insert(in_rec)
                        else:
                            # records: [row_dict, ...]
                            for row_dict in records:
                                if index not in row_dict:
                                    continue
                                ok, pt = self._as_point(row_dict[index])
                                if not ok:
                                    continue
                                in_rec = {"pos": row_dict[self.primary_key], index: pt, "deleted": False}
                                rt.insert(in_rec)
                    except Exception as e:
                        if DEBUG_IDX: print("[RTREE insert secondary] skip:", e)

        # Devolver 'records' para que el executor compute meta.affected
        return records

    # ----------------------------------- DML search ---------------------------------- #

    def search(self, params: dict):
        """Búsqueda por igualdad. Si el campo es primario o no está indexado, usa el primario."""
        field = params["field"]
        value = params["value"]
        records = []

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        mainindex = False     # ¿vamos por el índice primario?
        same_key = True       # para SEQ/ISAM: si no coincide la clave, cambia estrategia

        # ¿el campo es PK/UNIQUE?
        if "key" in self.relation[field]:
            if self.relation[field]["key"] == "primary":
                additional["unique"] = True
                mainindex = True
            else:
                additional["unique"] = True

        # Si el campo NO está indexado secundariamente → caer al primario
        if field not in self.indexes:
            mainindex = True
            same_key = False

        if mainindex:
            # ----------- búsqueda en PRIMARIO -----------
            if mainindx == "heap":
                records = HeapFile(mainfilename).search(additional)
            elif mainindx == "sequential":
                records = SeqFile(mainfilename).search(additional, same_key)
            elif mainindx == "isam":
                records = IsamFile(mainfilename).search(additional, same_key)
            elif mainindx == "b+":
                try:
                    bp = BPlusFile(mainfilename)
                    if field == self.primary_key:
                        hits = bp.search({"key": field, "value": value})
                        records = [hits] if isinstance(hits, dict) else (hits or [])
                    else:
                        # Fallback: escaneo completo si el campo no es PK
                        rows = self.execute({"op": "scan"}) or []
                        records = [r for r in rows if isinstance(r, dict) and r.get(field) == value]
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS search primary] skip:", e)
                    records = []
        else:
            # ----------- búsqueda por INDICE SECUNDARIO (protegida) -----------
            filename = self.indexes[field]["filename"]
            kind = self.indexes[field]["index"]

            if kind == "hash":
                try:
                    h = ExtendibleHashingFile(filename)
                    hit = h.find(value, key_name=field)
                    records = []
                    if hit:
                        if self.indexes["primary"]["index"] == "heap":
                            if "pos" in hit:
                                records = [{"pos": hit["pos"]}]
                        else:
                            records = [hit]
                except Exception as e:
                    if DEBUG_IDX: print("[HASH search secondary] skip:", e)
                    records = []

            elif kind == "b+":
                try:
                    bp = BPlusFile(filename)
                    hits = bp.search({"key": field, "value": value})
                    records = []
                    if self.indexes["primary"]["index"] == "heap":
                        for h in (hits or []):
                            if "pos" in h:
                                records.append({"pos": h["pos"]})
                    else:
                        records = hits or []
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS search secondary] skip:", e)
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
                    return self._bridge_from_rtree(items)
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE search secondary] skip:", e)
                    return []

            # Si primario es HEAP y lo que tenemos son posiciones → resolver a filas
            if self.indexes["primary"]["index"] == "heap":
                return HeapFile(mainfilename).search_by_pos(self._posify(records))

            # Si primario NO es HEAP → resolver a filas por PK
            ret_records = []
            for rec in records:
                ret_records.extend(
                    self.search({"op": "search", "field": self.primary_key, "value": rec["pk"]})
                )
            records = ret_records

        return records

    # ------------------------------- DML range_search ------------------------------- #

    def range_search(self, params: dict):
        """Búsqueda por rango (min/max) similar a search, con ramas para secundarios."""
        field = params["field"]
        additional = {"key": field}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        mainindex = False
        same_key = True
        records = []

        if "key" in self.relation[field] and self.relation[field]["key"] == "primary":
            mainindex = True

        if field not in self.indexes:
            mainindex = True
            same_key = False

        if mainindex:
            # ----------- rango en PRIMARIO -----------
            if mainindx == "heap":
                additional["min"] = params["min"]; additional["max"] = params["max"]
                records = HeapFile(mainfilename).range_search(additional)
            elif mainindx == "sequential":
                additional["min"] = params["min"]; additional["max"] = params["max"]
                records = SeqFile(mainfilename).range_search(additional, same_key)
            elif mainindx == "isam":
                additional["min"] = params["min"]; additional["max"] = params["max"]
                records = IsamFile(mainfilename).range_search(additional, same_key)
            elif mainindx == "b+":
                try:
                    bp = BPlusFile(mainfilename)
                    if field == self.primary_key:
                        records = bp.range_search({"key": field, "min": params["min"], "max": params["max"]})
                    else:
                        # Fallback por escaneo si no es PK
                        lo, hi = params["min"], params["max"]
                        rows = self.execute({"op": "scan"}) or []
                        def in_range(v): return v is not None and lo <= v <= hi
                        records = [r for r in rows if isinstance(r, dict) and in_range(r.get(field))]
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS range_search primary] skip:", e)
                    records = []
        else:
            # ----------- rango por SECUNDARIO (protegido) -----------
            filename = self.indexes[field]["filename"]
            kind = self.indexes[field]["index"]

            if kind == "b+":
                try:
                    bp = BPlusFile(filename)
                    hits = bp.range_search({"key": field, "min": params["min"], "max": params["max"]})
                    if self.indexes["primary"]["index"] == "heap":
                        # Resolver posiciones a filas
                        return HeapFile(mainfilename).search_by_pos(
                            self._posify([h["pos"] for h in (hits or []) if isinstance(h, dict) and "pos" in h])
                        )
                    else:
                        records = hits or []
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS range_search secondary] skip:", e)
                    records = []

            elif kind == "rtree":
                try:
                    # Si no viene rect armado, construirlo desde min/max simples
                    rect = params.get("rect") or {
                        "xmin": params["min"], "xmax": params["max"],
                        "ymin": params.get("ymin", params["min"]),
                        "ymax": params.get("ymax", params["max"]),
                    }
                    is_heap = (self.indexes["primary"]["index"] == "heap")
                    rt = self._make_rtree(field, heap_ok=is_heap)
                    items = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])
                    return self._bridge_from_rtree(items)
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE range_search secondary] skip:", e)
                    return []

            # Resolver a filas igual que en search()
            if self.indexes["primary"]["index"] == "heap":
                return HeapFile(mainfilename).search_by_pos(self._posify(records))
            ret_records = []
            for rec in records:
                ret_records.extend(
                    self.search({"op": "search", "field": self.primary_key, "value": rec["pk"]})
                )
            records = ret_records

        return records

    # ----------------------------------- DML knn ------------------------------------ #

    def knn(self, params: dict):
        """k-NN sobre un índice R-Tree secundario. Siempre protegido."""
        field = params["field"]
        if field not in self.indexes:
            return []
        if "key" in self.relation.get(field, {}) and self.relation[field]["key"] == "primary":
            return []
        if self.indexes[field]["index"] != "rtree":
            return []
        try:
            is_heap = (self.indexes["primary"]["index"] == "heap")
            rt = self._make_rtree(field, heap_ok=is_heap)
            items = rt.knn(params["point"][0], params["point"][1], params["k"])
            return self._bridge_from_rtree(items)
        except Exception as e:
            if DEBUG_IDX: print("[RTREE knn] skip:", e)
            return []

    # ----------------------------------- DML remove --------------------------------- #

    def remove(self, params):
        """Elimina por condición y limpia secundarios (todo protegido)."""
        field = params["field"]
        value = params["value"]

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        mainindex = False
        same_key = True
        records = []

        # ¿La condición es sobre la PK? → vamos al primario.
        if "key" in self.relation.get(field, {}):
            if self.relation[field]["key"] == "primary":
                additional["unique"] = True
                mainindex = True
            else:
                additional["unique"] = True

        if field not in self.indexes:
            mainindex = True
            same_key = False

        # 1) Borrado en PRIMARIO o selección de víctimas por SECUNDARIO
        if mainindex:
            if mainindx == "heap":
                records = HeapFile(mainfilename).remove(additional)
            elif mainindx == "sequential":
                records = SeqFile(mainfilename).remove(additional, same_key)
            elif mainindx == "isam":
                records = IsamFile(mainfilename).remove(additional, same_key)
            elif mainindx == "b+":
                try:
                    bp = BPlusFile(mainfilename)
                    if field == self.primary_key:
                        bp.remove({"key": self.primary_key, "value": value})
                        records = [{self.primary_key: value}]
                    else:
                        victims = [r for r in (self.execute({"op": "scan"}) or []) if isinstance(r, dict) and r.get(field) == value]
                        for v in victims:
                            try:
                                bp.remove({"key": self.primary_key, "value": v[self.primary_key]})
                            except Exception:
                                pass
                        records = victims
                except Exception as e:
                    if DEBUG_IDX: print("[BPLUS remove primary] skip:", e)
                    records = []
        else:
            # Buscar víctimas por SECUNDARIO (protegido)
            search_index = self.indexes[field]["index"]
            filename = self.indexes[field]["filename"]
            try:
                if search_index == "hash":
                    h = ExtendibleHashingFile(filename)
                    hit = h.find(value, key_name=field)
                    if hit:
                        if mainindx == "heap" and "pos" in hit:
                            records = [hit["pos"]]
                        else:
                            records = [hit]
                elif search_index == "b+":
                    bp = BPlusFile(filename)
                    hits = bp.search({"key": field, "value": value}) or []
                    if mainindx == "heap":
                        records = [h["pos"] for h in hits if isinstance(h, dict) and "pos" in h]
                    else:
                        records = hits
                elif search_index == "rtree":
                    is_heap = (mainindx == "heap")
                    rt = self._make_rtree(field, heap_ok=is_heap)
                    items = []
                    if isinstance(value, (list, tuple)) and len(value) >= 2:
                        x, y = float(value[0]), float(value[1]); eps = 1e-9
                        items = rt.search_rect(x-eps, x+eps, y-eps, y+eps)
                    elif isinstance(value, dict) and {"xmin","xmax","ymin","ymax"} <= set(value.keys()):
                        items = rt.search_rect(value["xmin"], value["xmax"], value["ymin"], value["ymax"])
                    records = self._bridge_from_rtree(items)
            except Exception as e:
                if DEBUG_IDX: print("[SECONDARY pre-remove] skip:", e)
                records = []

            # Aplicar eliminación en PRIMARIO con las víctimas halladas
            if mainindx == "heap":
                return HeapFile(mainfilename).delete_by_pos(self._posify(records))
            else:
                temp_records = []
                if mainindx == "b+":
                    try:
                        bp = BPlusFile(mainfilename)
                        for rec in (records or []):
                            pk = rec.get("pk") if isinstance(rec, dict) else None
                            if pk is None:
                                continue
                            try:
                                bp.remove({"key": self.primary_key, "value": pk})
                                temp_records.append(rec)
                            except Exception:
                                pass
                    except Exception as e:
                        if DEBUG_IDX: print("[BPLUS remove primary apply] skip:", e)
                else:
                    for rec in (records or []):
                        pk = rec.get("pk") if isinstance(rec, dict) else None
                        if pk is None:
                            continue
                        add = {"key": self.primary_key, "value": pk, "unique": True}
                        if mainindx == "sequential":
                            temp_records.extend(SeqFile(mainfilename).remove(add, True))
                        elif mainindx == "isam":
                            temp_records.extend(IsamFile(mainfilename).remove(add, True))
                records = temp_records

        # 2) Limpiar SECUNDARIOS (protegido; cualquier error se ignora)
        for index in self.indexes:
            if index == "primary" or self.indexes[index]["filename"] == mainfilename:
                continue
            kind = self.indexes[index]["index"]
            filename = self.indexes[index]["filename"]

            if kind == "hash":
                try:
                    h = ExtendibleHashingFile(filename)
                    for rec in (records or []):
                        if isinstance(rec, tuple) and len(rec) >= 2:  # (row, pos)
                            row = rec[0]
                            if index in row:
                                try: h.remove(row[index], key_name=index)
                                except Exception: pass
                        elif isinstance(rec, dict) and index in rec:   # dict
                            try: h.remove(rec[index], key_name=index)
                            except Exception: pass
                except Exception as e:
                    if DEBUG_IDX: print("[HASH remove secondary] skip:", e)

            elif kind == "b+":
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
                except Exception as e:
                    if DEBUG_IDX: print("[RTREE remove secondary] skip:", e)

        # Devolver lista de afectados (para meta.affected)
        return records

    # ----------------------------------- execute ------------------------------------ #

    def execute(self, params: dict):
        """Multiplexor de operaciones."""
        if params["op"] == "build":
            return self.build(params)

        elif params["op"] == "insert":
            return self.insert(params)

        elif params["op"] == "search":
            return self.search(params)

        elif params["op"] in ("range search", "range_search"):
            return self.range_search(params)

        elif params["op"] == "knn":
            return self.knn(params)

        elif params["op"] == "remove":
            return self.remove(params)

        elif params["op"] == "rtree_within_circle":
            # Op espacial (si falla, devolvemos vacío)
            try:
                field = params["field"]
                cx, cy = float(params["center"]["x"]), float(params["center"]["y"])
                rr = float(params["radius"])
                is_heap = (self.indexes["primary"]["index"] == "heap")
                rt = self._make_rtree(field, heap_ok=is_heap)
                items = rt.range(cx, cy, rr)
                return self._bridge_from_rtree(items)
            except Exception as e:
                if DEBUG_IDX: print("[RTREE within_circle] skip:", e)
                return []

        elif params["op"] == "rtree_range":
            # Op espacial rectangular (si falla, devolvemos vacío)
            try:
                field = params["field"]
                rect = params["rect"]
                is_heap = (self.indexes["primary"]["index"] == "heap")
                rt = self._make_rtree(field, heap_ok=is_heap)
                items = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])
                return self._bridge_from_rtree(items)
            except Exception as e:
                if DEBUG_IDX: print("[RTREE range op] skip:", e)
                return []

        elif params["op"] == "import_csv":
            # Importación desde CSV (simple casting + insert por fila)
            import csv, json as _json
            path = params["path"]
            inserted = 0
            with open(path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    rec = {}
                    for col, spec in self.relation.items():
                        if col not in row:
                            continue
                        v = row[col]
                        # parse JSON para coords u otras listas si vienen como "[...]"
                        if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                            try:
                                import json
                                v = json.loads(v)
                            except Exception:
                                pass
                        # cast básico según tipo
                        t = (spec.get("type") or "").lower()
                        if v == "" or v is None:
                            v = None
                        elif t in ("int", "i"):
                            v = int(v)
                        elif t in ("float", "real", "f", "double"):
                            v = float(v)
                        # string/varchar se deja tal cual
                        rec[col] = v
                    self.insert({"record": rec})
                    inserted += 1
            return {"count": inserted}

        elif params["op"] == "scan":
            # Lectura completa del archivo (cualquier primario) usando header binario
            main_kind = self.indexes["primary"]["index"]
            if main_kind == "b+":
                # Para B+ podemos hacer un range completo por la PK
                bp = BPlusFile(self.indexes["primary"]["filename"])
                pk = self.primary_key
                t = (self.relation.get(pk, {}).get("type") or "").lower()
                lo, hi = (float("-inf"), float("inf")) if t in ("int", "i", "float", "real", "double", "f") else ("", "\uffff")
                return bp.range_search({"key": pk, "min": lo, "max": hi})

            mainfilename = self.indexes["primary"]["filename"]
            records = []
            with open(mainfilename, "rb") as f:
                # header: [uint32 schema_len][schema_json][...registros...]
                slen = struct.unpack("I", f.read(4))[0]
                schema = _json.loads(f.read(slen).decode("utf-8"))
                fmt = build_format(schema)
                rec_size = struct.calcsize(fmt)
                end = f.seek(0, 2)  # EOF
                f.seek(4 + slen)   # inicio de datos

                while f.tell() < end:
                    chunk = f.read(rec_size)
                    if not chunk or len(chunk) < rec_size:
                        break
                    rec = Record.unpack(chunk, fmt, schema).fields
                    if rec.get("deleted"):
                        continue
                    records.append(rec)
            return records