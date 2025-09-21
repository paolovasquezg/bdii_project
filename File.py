from Methods import get_json, get_filename, put_json
from Record import Record
from Heap import HeapFile
import os

class File:

    def __init__(self, table: str):
        self.filename = get_filename(table)
        self.relation, self.indexes = get_json(self.filename, 2)


    def p_print(self, name, record, additional, filename = ""):
         print(name)
         print(record)
         print(additional)
         print(filename)

    
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
            self.p_print("seq", record, additional,mainfilename) 
        elif (maindex == "isam"):
            self.p_print("isam", record, additional,mainfilename) 
        elif (maindex == "b+"):
            self.p_print("b+", record, additional, mainfilename) 
            
        if len(record) >= 1:

            for index in self.indexes:

                if index == "primary" or self.indexes[index]["filename"]  == mainfilename:
                    continue
                
                if len(records) > 1:
                    filename = self.indexes[index]["filename"]
                    schema = get_json(filename)                 
                    os.remove(filename)
                    put_json(filename, [schema])

                filename = self.indexes[index]["filename"]
                additional = {"key": index}

                for record in records:
            
                    new_record = {"pos": record[1], "deleted": False}
                    new_record[index] = record[0][index]

                    indx = self.indexes[index]["index"]

                    if (indx  == "hash"):
                        self.p_print("hash", new_record,additional,filename) 
                    elif (indx == "b+"):
                        self.p_print("b+", new_record,additional,filename) 
                    elif (indx == "rtree"):
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
                self.p_print("seq", additional, mainfilename) 
            elif (mainindx == "isam"):
                self.p_print("isam", additional, mainfilename) 
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
                self.p_print("rtree", additional, filename) 
        
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
                self.p_print("seq", additional, mainfilename)
            elif (mainindx == "isam"):
                additional["min"] = params["min"]
                additional["max"] = params["max"]
                self.p_print("isam", additional, mainfilename)
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
                additional["point"] = params["point"]
                additional["r"] = params["r"]
                self.p_print("rtree", additional, filename)
        
        return records
    
    def knn(self, params: dict):
        field = params["field"]

        additional = {"key": field, "point": params["point"], "k": params["k"]}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        mainindex = False
        records = []

        if "key" in self.relation[field] and self.relation[field]["key"] == "primary":
                mainindex = True
        
        if field not in self.indexes:
            return []
        
        if mainindex:
            if (mainindx == "rtree"):
                self.p_print("rtree", additional, mainfilename)
        else:
            
            filename = self.indexes[field]["filename"]
            indx = self.indexes[field]["index"]

            if (indx == "rtree"):
                self.p_print("rtree", additional, filename)
        
        return records
    
    def remove(self, params):
        field = params["field"]
        value = params["value"]

        additional = {"key": field, "value": value, "unique": False}
        mainfilename = self.indexes["primary"]["filename"]
        mainindx = self.indexes["primary"]["index"]
        same_key = True
        records = []

        if "key" in self.relation[field]:
            if (self.relation[field]["key"] == "primary") or self.relation[field]["key"] == "unique":
                additional["unique"] = True
        
        if field not in self.indexes:
            same_key = False
        
        records = []
        reconstructed = False

        if (mainindx == "heap"):
            DeleteFile = HeapFile(mainfilename)
            records = DeleteFile.remove(additional)
        elif (mainindx  == "sequential"):
            self.p_print("seq", additional, mainfilename)
        elif (mainindx == "isam"):
            self.p_print("isam", additional, mainfilename)
        elif (mainindx == "b+"):
            self.p_print("b+", additional, mainfilename)
        
        for index in self.indexes:

            if index == "primary" or self.indexes[index]["filename"]  == mainfilename:
                    continue
            
            filename = self.indexes[index]["filename"]
            indx = self.indexes[index]["index"]

            if (reconstructed):
                schema = get_json(filename)                 
                os.remove(filename)
                put_json(filename, [schema])

            for record in records:

                if (reconstructed == False):

                    additional = {"key": index, "value": record[index], "unique": False}

                    if "key" in self.relation[index]:
                        if (self.relation[index]["key"] == "primary") or self.relation[index]["key"] == "unique":
                            additional["unique"] = True
                    
                    if (indx  == "hash"):
                        self.p_print("hash",additional,filename) 
                    elif (indx == "b+"):
                        self.p_print("b+", additional,filename) 
                    elif (indx == "rtree"):
                        self.p_print("rtree", additional,filename)
                
                else:
                    new_record = {"pos": record[1], "deleted": False}
                    new_record[index] = record[0][index]
                    additional = {"key": index}

                    if (indx  == "hash"):
                        self.p_print("hash",new_record,additional,filename) 
                    elif (indx == "b+"):
                        self.p_print("b+", new_record, additional,filename) 
                    elif (indx == "rtree"):
                        self.p_print("rtree", new_record, additional, filename)

    def execute(self, params: dict):

        if params["op"] == "insert":
            self.insert(params)
        
        elif params["op"] == "search":
            return self.search(params)
        
        elif params["op"] == "range search":
            return self.range_search(params)
        
        elif params["op"] == "knn":
            self.knn(params)
        
        else:
            self.remove(params)