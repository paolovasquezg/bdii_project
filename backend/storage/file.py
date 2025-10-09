from backend.catalog.catalog import get_json, get_filename
from backend.storage.primary.heap import HeapFile
from backend.storage.primary.sequential import SeqFile
from backend.storage.primary.isam import IsamFile
from backend.storage.secondary.rtree import RTree
import os

DEBUG_IDX = os.getenv("BD2_DEBUG_INDEX", "0").lower() in ("1", "true", "yes")

class File:
    def get_pk(self):
        for field in self.relation:
            if "key" in self.relation[field] and (self.relation[field]["key"] == "primary"):
                return field

    def __init__(self, table: str):
        self.filename = get_filename(table)
        self.relation, self.indexes = get_json(self.filename, 2)
        self.primary_key = self.get_pk()
        self.table = table

    def p_print(self, name, record, additional, filename = ""):
        if not DEBUG_IDX:
            return
        print(name)
        print(record)
        print(additional)
        print(filename)

    def _primary(self):
        """Devuelve (kind, filename) del índice primario."""
        p = self.indexes["primary"]
        return p["index"], p["filename"]

    def _make_rtree(self, field: str, *, heap_ok: bool):
        """Construye el adapter RTree con data_dir derivado del filename del índice."""
        idx_meta = self.indexes[field]
        idx_filename = idx_meta["filename"]
        # data_dir = .../runtime/files   (padre de la carpeta <tabla>/)
        data_dir = os.path.dirname(os.path.dirname(idx_filename))
        return RTree(
            self.table, field, data_dir,
            key=field,
            M=idx_meta.get("M", 32),
            heap_file=(self.indexes["primary"]["filename"] if heap_ok else None)
        )

    def _bridge_from_rtree(self, items: list):
        """Si el primario NO es heap, 'items' trae {'pos': pk}. Los traducimos a filas."""
        if self.indexes["primary"]["index"] == "heap":
            # El adapter ya devolvió filas completas del heap
            return items or []
        out = []
        for it in (items or []):
            pk = (it or {}).get("pos")
            if pk is None:
                continue
            out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
        return out

    def build(self, params):
        if self.indexes["primary"]["index"] != "isam":
            for record in params["records"]:
                self.insert({"op": "insert", "record": record})    
        else:
            mainfilename = self.indexes["primary"]["filename"]
            additional = {"key": None, "unique": []}
            records = []

            for index in self.indexes:
                if self.indexes[index]["filename"] == mainfilename and index != "primary":
                    additional["key"] = index
                    break
        
            for field in self.relation:
                if "key" in self.relation[field] and (self.relation[field]["key"] == "primary" or self.relation[field]["key"] == "unique"):
                        additional["unique"].append(field)
            
            mainfilename = self.indexes["primary"]["filename"]

            BuildFile = IsamFile(mainfilename)
            records = BuildFile.build(params["records"], additional)

            for index in self.indexes:

                if index == "primary" or self.indexes[index]["filename"]  == mainfilename:
                    continue
                
                filename = self.indexes[index]["filename"]
                additional = {"key": index}

                for record in records:

                    new_record = {"pk": record[self.primary_key], "deleted": False}
                    new_record[index] = record[index]

                    indx = self.indexes[index]["index"]

                    if (indx  == "hash"):
                        self.p_print("hash", new_record,additional,filename) 
                    elif (indx == "b+"):
                        self.p_print("b+", new_record,additional,filename)

                    elif (indx == "rtree"):
                        # Construimos el R-Tree usando la PK como 'pos'
                        idx_filename = self.indexes[index]["filename"]
                        # data_dir = .../runtime/files (padre de la carpeta <tabla>/)
                        data_dir = os.path.dirname(os.path.dirname(idx_filename))
                        rt = RTree(self.table, index, data_dir, key=index, M=self.indexes[index].get("M", 32))

                        for rec in records:  # 'records' aquí son dicts (build ISAM)
                            if index not in rec:
                                continue
                            in_rec = {"pos": rec[self.primary_key], index: rec[index], "deleted": False}
                            rt.insert(in_rec)

    def insert(self, params):
        
        mainfilename = self.indexes["primary"]["filename"]
        record = params["record"]
        record["deleted"] = False
        additional = {"key": None, "unique": []}
        records = []

        for index in self.indexes:
            if self.indexes[index]["filename"] == mainfilename and index != "primary":
                additional["key"] = index
                break
        
        for field in self.relation:
            if "key" in self.relation[field] and (self.relation[field]["key"] == "primary" or self.relation[field]["key"] == "unique"):
                    additional["unique"].append(field)
        
        maindex = self.indexes["primary"]["index"]
        mainfilename = self.indexes["primary"]["filename"]

        if (maindex == "heap"):
            InsFile = HeapFile(mainfilename)
            records = InsFile.insert(record,additional)
        elif (maindex == "sequential"):
            InsFile = SeqFile(mainfilename)
            records = InsFile.insert(record,additional)
        elif (maindex == "isam"):
            InsFile = IsamFile(mainfilename)
            records = InsFile.insert(record,additional) 
        elif (maindex == "b+"):
            self.p_print("b+", record, additional, mainfilename) 
            
        if len(records) >= 1:

            for index in self.indexes:

                if index == "primary" or self.indexes[index]["filename"]  == mainfilename:
                    continue
                
                filename = self.indexes[index]["filename"]
                additional = {"key": index}

                for record in records:

                    new_record = {}

                    if self.indexes["primary"]["index"] == "heap":
                        new_record = {"pos": record[1], "deleted": False}
                        new_record[index] = record[0][index]
                    
                    else:
                        new_record = {"pk": record[self.primary_key], "deleted": False}
                        new_record[index] = record[index]

                    indx = self.indexes[index]["index"]

                    if (indx  == "hash"):
                        self.p_print("hash", new_record,additional,filename) 
                    elif (indx == "b+"):
                        self.p_print("b+", new_record,additional,filename)
                    elif (indx == "rtree"):
                        is_heap = (self.indexes["primary"]["index"] == "heap")
                        idx_filename = self.indexes[index]["filename"]
                        data_dir = os.path.dirname(os.path.dirname(idx_filename))
                        rt = RTree(self.table, index, data_dir,
                                   key=index, M=self.indexes[index].get("M", 32),
                                   heap_file=self.indexes["primary"]["filename"] if is_heap else None)

                        if is_heap:
                            # records: [(row_dict, pos), ...]
                            for row_dict, pos in records:
                                if index not in row_dict:
                                    continue
                                in_rec = {"pos": pos, index: row_dict[index], "deleted": False}
                                val = in_rec.get("coords")
                                if isinstance(in_rec.get("coords"), str) and in_rec["coords"].startswith("["):
                                    import json
                                    in_rec["coords"] = json.loads(in_rec["coords"])
                                rt.insert(in_rec)

                        else:
                            # records: [row_dict, ...]
                            for row_dict in records:
                                if index not in row_dict:
                                    continue
                                in_rec = {"pos": row_dict[self.primary_key], index: row_dict[index], "deleted": False}
                                val = in_rec.get("coords")
                                if isinstance(in_rec.get("coords"), str) and in_rec["coords"].startswith("["):
                                    import json
                                    in_rec["coords"] = json.loads(in_rec["coords"])
                                rt.insert(in_rec)

    def search(self, params: dict):
        field = params["field"]
        value = params["value"]
        records = []

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        mainindex = False
        same_key = True

        if "key" in self.relation[field]:
            if (self.relation[field]["key"] == "primary"):
                additional["unique"] = True
                mainindex = True
            
            else:
                additional["unique"] = True
        
        if field not in self.indexes:
            mainindex = True
            same_key = False
        
        if mainindex:

            if (mainindx == "heap"):
                SearchFile = HeapFile(mainfilename)
                records = SearchFile.search(additional)
            elif (mainindx  == "sequential"):
                SearchFile = SeqFile(mainfilename)
                records = SearchFile.search(additional, same_key)
            elif (mainindx == "isam"):
                SearchFile = IsamFile(mainfilename)
                records = SearchFile.search(additional, same_key)
            elif (mainindx == "b+"):
                self.p_print("b+", additional, mainfilename) 
        
        else:
            
            filename = self.indexes[field]["filename"]
            indx = self.indexes[field]["index"]

            if (indx  == "hash"):
                self.p_print("hash", additional, filename) 
            elif (indx == "b+"):
                self.p_print("b+", additional, filename)
            elif (indx == "rtree"):
                # Soportamos dos formas:
                # 1) BETWEEN numérico (no aplica a rtree, se ignora aquí)
                # 2) Rectángulo para rtree (cuando params trae point/r lo maneja otra op)
                if "point" in params and "r" in params:
                    # Esto lo tratamos en un op dedicado (rtree_within_circle)
                    records = []
                else:
                    rect = params.get("rect") or {
                        "xmin": params["min"], "xmax": params["max"],
                        "ymin": params.get("ymin", params["min"]),
                        "ymax": params.get("ymax", params["max"])
                    }
                    is_heap = (self.indexes["primary"]["index"] == "heap")
                    rt = self._make_rtree(field, heap_ok=is_heap)
                    records = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])
                    records = self._bridge_from_rtree(records)

            if self.indexes["primary"]["index"] == "heap":
                SearchFile = HeapFile(mainfilename)
                return SearchFile.search_by_pos(records)

            ret_records = []

            for record in records:
                ret_records.extend(
                    self.search({"op": "search", "field": self.primary_key, "value": record[self.primary_key]}))

            records = ret_records
        
        return records
    
    def range_search(self, params: dict):
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

            if (mainindx == "heap"):
                additional["min"] = params["min"]
                additional["max"] = params["max"]
                RangeFile = HeapFile(mainfilename)
                records = RangeFile.range_search(additional)
            elif (mainindx  == "sequential"):
                additional["min"] = params["min"]
                additional["max"] = params["max"]
                RangeFile = SeqFile(mainfilename)
                records = RangeFile.range_search(additional, same_key)
            elif (mainindx == "isam"):
                additional["min"] = params["min"]
                additional["max"] = params["max"]
                RangeFile = IsamFile(mainfilename)
                records = RangeFile.range_search(additional, same_key)
            elif (mainindx == "b+"):
                additional["min"] = params["min"]
                additional["max"] = params["max"]
                self.p_print("b+", additional, mainfilename)
        
        else:
            
            filename = self.indexes[field]["filename"]
            indx = self.indexes[field]["index"]

            if (indx == "b+"):
                additional["min"] = params["min"]
                additional["max"] = params["max"]
                self.p_print("b+", additional, filename)
            elif (indx == "rtree"):
                # params puede traer rect ya armado; si no, lo armamos con min/max
                rect = params.get("rect") or {
                    "xmin": params["min"], "xmax": params["max"],
                    "ymin": params.get("ymin", params["min"]),
                    "ymax": params.get("ymax", params["max"]),
                }
                is_heap = (self.indexes["primary"]["index"] == "heap")
                idx_filename = self.indexes[field]["filename"]
                data_dir = os.path.dirname(os.path.dirname(idx_filename))
                rt = RTree(self.table, field, data_dir,
                           key=field, M=self.indexes[field].get("M", 32),
                           heap_file=self.indexes["primary"]["filename"] if is_heap else None)

                items = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])

                if is_heap:
                    return items  # ya son filas desde heap
                else:
                    # bridge PK -> filas
                    out = []
                    for it in (items or []):
                        pk = (it or {}).get("pos")
                        if pk is None:
                            continue
                        out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
                    return out

            if self.indexes["primary"]["index"] == "heap":
                SearchFile = HeapFile(mainfilename)
                return SearchFile.search_by_pos(records)
            
            ret_records = []

            for record in records:
                ret_records.extend(
                    self.search({"op": "search", "field": self.primary_key, "value": record[self.primary_key]}))

            records = ret_records
        
        return records

    def knn(self, params: dict):
        field = params["field"]
        if field not in self.indexes:
            return []
        if "key" in self.relation.get(field, {}) and self.relation[field]["key"] == "primary":
            return []

        if self.indexes[field]["index"] != "rtree":
            return []

        is_heap = (self.indexes["primary"]["index"] == "heap")
        idx_filename = self.indexes[field]["filename"]
        data_dir = os.path.dirname(os.path.dirname(idx_filename))
        rt = RTree(self.table, field, data_dir,
                   key=field, M=self.indexes[field].get("M", 32),
                   heap_file=self.indexes["primary"]["filename"] if is_heap else None)

        items = rt.knn(params["point"][0], params["point"][1], params["k"])

        if is_heap:
            return items
        else:
            out = []
            for it in (items or []):
                pk = (it or {}).get("pos")
                if pk is None:
                    continue
                out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
            return out

    def remove(self, params):
        field = params["field"]
        value = params["value"]

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        same_key = True
        mainindex = False
        search_index = ""
        records = []

        if "key" in self.relation[field]:
            if (self.relation[field]["key"] == "primary"):
                additional["unique"] = True
                mainindex = True
            
            else:
                additional["unique"] = True
        
        if field not in self.indexes:
            same_key = False
            mainindex = True
        
        records = []
        
        if mainindex:

            if (mainindx == "heap"):
                DeleteFile = HeapFile(mainfilename)
                records = DeleteFile.remove(additional)
            elif (mainindx  == "sequential"):
                DeleteFile = SeqFile(mainfilename)
                records = DeleteFile.remove(additional, same_key)
            elif (mainindx == "isam"):
                DeleteFile = IsamFile(mainfilename)
                records = DeleteFile.remove(additional, same_key)
            elif (mainindx == "b+"):
                self.p_print("b+", additional, mainfilename)
        
        else:

            search_index = self.indexes[field]["index"]
            filename = self.indexes[field]["filename"]

            if (search_index  == "hash"):
                self.p_print("hash",additional,filename) 
            elif (search_index == "b+"):
                self.p_print("b+", additional,filename) 
            elif (search_index == "rtree"):
                self.p_print("rtree", additional,filename)
            

            if mainindx == "heap":

                DeleteFile = HeapFile(mainfilename)
                records = DeleteFile.delete_by_pos(records)
            
            else:

                temp_records = []

                for record in records:

                    additional = {"key": self.primary_key, "value": record["pk"], "unique": True}
                    same_key = True

                    if (mainindx  == "sequential"):
                        DeleteFile = SeqFile(mainfilename)
                        temp_records.extend(DeleteFile.remove(additional, same_key))
                    elif (mainindx == "isam"):
                        DeleteFile = IsamFile(mainfilename)
                        temp_records.extend(DeleteFile.remove(additional, same_key))
                    elif (mainindx == "b+"):
                        self.p_print("b+", additional, mainfilename)

                records = temp_records


        for index in self.indexes:

            if index == "primary" or self.indexes[index]["filename"]  == mainfilename:
                    continue
            
            filename = self.indexes[index]["filename"]
            indx = self.indexes[index]["index"]

            if indx == search_index:
                continue

            for record in records:

                additional = {"key": index, "value": record[index], "unique": True}

                if "key" in self.relation[index]:
                    if (self.relation[index]["key"] == "primary") or self.relation[index]["key"] == "unique":
                        additional["unique"] = True
                
                if (indx  == "hash"):
                    self.p_print("hash",additional,filename) 
                elif (indx == "b+"):
                    self.p_print("b+", additional,filename)
                elif (indx == "rtree"):
                    is_heap = (self.indexes["primary"]["index"] == "heap")
                    idx_filename = self.indexes[index]["filename"]
                    data_dir = os.path.dirname(os.path.dirname(idx_filename))
                    rt = RTree(self.table, index, data_dir,
                               key=index, M=self.indexes[index].get("M", 32),
                               heap_file=self.indexes["primary"]["filename"] if is_heap else None)

                    for rec in records:
                        # Si viene como (row, pos)
                        if isinstance(rec, tuple) and len(rec) >= 2:
                            row, pos = rec[0], rec[1]
                            rm = {"pos": pos}
                            if index in row: rm[index] = row[index]
                            try:
                                rt.remove(rm)
                            except:
                                pass
                        # Si viene como dict (con PK)
                        elif isinstance(rec, dict):
                            rm = {}
                            if "pos" in rec:
                                rm["pos"] = rec["pos"]
                            elif self.primary_key in rec:
                                rm["pos"] = rec[self.primary_key]
                            if index in rec: rm[index] = rec[index]
                            if "pos" in rm:
                                try:
                                    rt.remove(rm)
                                except:
                                    pass

    def execute(self, params: dict):

        if params["op"] == "build":
            self.build(params)

        elif params["op"] == "insert":
            self.insert(params)
        
        elif params["op"] == "search":
            return self.search(params)
        
        elif params["op"] == "range search":
            return self.range_search(params)
        
        elif params["op"] == "knn":
            self.knn(params)
        
        elif params["op"] == "remove":
            self.remove(params)

        elif params["op"] == "rtree_within_circle":
            field = params["field"]
            cx, cy = float(params["center"]["x"]), float(params["center"]["y"])
            rr = float(params["radius"])

            is_heap = (self.indexes["primary"]["index"] == "heap")
            idx_filename = self.indexes[field]["filename"]
            data_dir = os.path.dirname(os.path.dirname(idx_filename))

            rt = RTree(self.table, field, data_dir,
                       key=field, M=self.indexes[field].get("M", 32),
                       heap_file=self.indexes["primary"]["filename"] if is_heap else None)

            items = rt.range(cx, cy, rr)
            if is_heap:
                return items

            out = []
            for it in (items or []):
                pk = (it or {}).get("pos")
                if pk is None: continue
                out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
            return out

        elif params["op"] == "rtree_range":
            field = params["field"]
            rect = params["rect"]
            is_heap = (self.indexes["primary"]["index"] == "heap")
            idx_filename = self.indexes[field]["filename"]
            data_dir = os.path.dirname(os.path.dirname(idx_filename))

            rt = RTree(self.table, field, data_dir,
                       key=field, M=self.indexes[field].get("M", 32),
                       heap_file=self.indexes["primary"]["filename"] if is_heap else None)

            items = rt.search_rect(rect["xmin"], rect["xmax"], rect["ymin"], rect["ymax"])
            if is_heap:
                return items

            out = []
            for it in (items or []):
                pk = (it or {}).get("pos")
                if pk is None: continue
                out.extend(self.search({"op": "search", "field": self.primary_key, "value": pk}))
            return out

        elif params["op"] == "import_csv":
            import csv
            prim = self.indexes.get("primary", {})
            # 2.1) ISAM → inserts fila a fila
            if prim.get("index") == "isam":
                # orden de columnas según schema
                order = [f["name"] for f in self.relation] if isinstance(self.relation, list) else list(
                    self.relation.keys())
                count = 0
                with open(params["path"], newline="", encoding="utf-8") as f:
                    r = csv.DictReader(f)
                    for row in r:
                        rec = {k: row[k] for k in order if k in row}
                        # casteos simples según tipo
                        for k in order:
                            t = str(self.relation[k]["type"]).lower() if isinstance(self.relation, dict) else str(
                                [x for x in self.relation if x["name"] == k][0]["type"]).lower()
                            if t in ("int", "integer"):
                                rec[k] = int(rec[k])
                            elif t in ("float", "real", "double", "double precision"):
                                rec[k] = float(rec[k])
                        self.insert({"op": "insert", "record": rec})
                        count += 1
                return {"ok": True, "count": count}

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





