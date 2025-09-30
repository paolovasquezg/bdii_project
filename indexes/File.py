from methods.Methods import get_json, get_filename, put_json
from indexes.Heap import HeapFile
from indexes.Sequential import SeqFile
from indexes.Isam import IsamFile
from indexes.rtree.RTree import RTreeFile

class File:

    def get_pk(self):
        for field in self.relation:
            if "key" in self.relation[field] and (self.relation[field]["key"] == "primary"):
                return field

    def __init__(self, table: str):
        self.filename = get_filename(table)
        self.relation, self.indexes = get_json(self.filename, 2)
        self.primary_key = self.get_pk()

    def p_print(self, name, record, additional, filename = ""):
         print(name)
         print(record)
         print(additional)
         print(filename)


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
                        # En build con ISAM no tenemos RID (pos); mantenemos el stub para no romper tu flujo
                        self.p_print("rtree", new_record,additional,filename)
                        

    def insert(self, params):
        
        mainfilename = self.indexes["primary"]["filename"]
        record = params["record"]
        record["deleted"] = False

        # NUEVO: guardamos el original con la geometría para usarlo al insertar en R-Tree
        orig = record.copy()

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

        # NUEVO: detectar todas las columnas con índice R-Tree
        rtree_fields = [k for k, v in self.indexes.items() if k != "primary" and v["index"] == "rtree"]

        if (maindex == "heap"):
            InsFile = HeapFile(mainfilename)
            # NUEVO: NO enviar al heap las columnas espaciales (arrays) para evitar struct.pack error
            heap_record = record.copy()
            for geo in rtree_fields:
                heap_record.pop(geo, None)
            records = InsFile.insert(heap_record,additional)
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
                        # record == (form_record.fields, pos)
                        new_record = {"pos": record[1], "deleted": False}
                        # si el índice secundario es R-Tree, usar la geometría del insert original
                        if index in rtree_fields and index in orig:
                            new_record[index] = orig[index]
                        else:
                            # para otros índices, tomar del registro empacado (sin campos espaciales)
                            new_record[index] = record[0].get(index)
                    
                    else:
                        new_record = {"pk": record[self.primary_key], "deleted": False}
                        new_record[index] = record[index]

                    indx = self.indexes[index]["index"]

                    if (indx  == "hash"):
                        self.p_print("hash", new_record,additional,filename) 
                    elif (indx == "b+"):
                        self.p_print("b+", new_record,additional,filename) 
                    elif (indx == "rtree"):
                        # Insertamos en el R-Tree solo si tenemos RID (pos) y geometría
                        if "pos" in new_record and (index in new_record) and (new_record[index] is not None):
                            try:
                                R = RTreeFile(filename); R.open()
                                R.insert(new_record, {"key": index})
                                R.close()
                            except Exception as e:
                                self.p_print("rtree_error", {"error": str(e)}, additional, filename)
                        else:
                            # Si el primario no es heap, dejamos el stub
                            self.p_print("rtree", new_record,additional,filename)
                     
    
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
                # búsqueda puntual por igualdad no aplica al R-Tree;
                # conservamos el stub para no romper flujos existentes
                self.p_print("rtree", additional, filename) 

            if self.indexes["primary"]["index"] == "heap":
                SearchFile = HeapFile(mainfilename)
                return SearchFile.search_by_pos(records)

            ret_records = []

            for record in records:
                ret_records.extend(self.search({"op": "insert", "field": self.primary_key, "value": record[self.primary_key]}))
            
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
                # llamada real al R-Tree (devolvemos directamente)
                try:
                    R = RTreeFile(filename); R.open()
                    out = R.range_search({"point": params["point"], "r": params["r"], "heap": mainfilename})
                    R.close()
                    return out
                except Exception as e:
                    self.p_print("rtree_error", {"error": str(e)}, {"key": field, "point": params.get("point"), "r": params.get("r")}, filename)

            if self.indexes["primary"]["index"] == "heap":
                SearchFile = HeapFile(mainfilename)
                return SearchFile.search_by_pos(records)
            
            ret_records = []

            for record in records:
                ret_records.extend(self.search({"op": "insert", "field": self.primary_key, "value": record[self.primary_key]}))
            
            records = ret_records
        
        return records
    
    def knn(self, params: dict):
        field = params["field"]

        additional = {"key": field, "point": params["point"], "k": params["k"]}
        mainindex = False
        records = []

        if "key" in self.relation[field] and self.relation[field]["key"] == "primary":
                mainindex = True
        
        if field not in self.indexes:
            return []
        
        if mainindex:
            return []
        
        else:
            
            filename = self.indexes[field]["filename"]
            indx = self.indexes[field]["index"]

            if (indx == "rtree"):
                # llamada real al R-Tree (devolvemos directamente)
                try:
                    R = RTreeFile(filename); R.open()
                    out = R.knn({"point": params["point"], "k": params["k"], "heap": self.indexes["primary"]["filename"]})
                    R.close()
                    return out
                except Exception as e:
                    self.p_print("rtree_error", {"error": str(e)}, additional, filename)

            if self.indexes["primary"]["index"] == "heap":
                SearchFile = HeapFile(self.indexes["primary"]["filename"])
                return SearchFile.search_by_pos(records)
            
            ret_records = []

            for record in records:
                ret_records.extend(self.search({"op": "insert", "field": self.primary_key, "value": record[self.primary_key]}))
            
            records = ret_records
        
        return records
    
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
            

            if mainindex == "heap":

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
                    self.p_print("rtree", additional,filename)
                

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
            return self.knn(params)
