from methods.Methods import get_json, build_format
from methods.Record import Record
import struct

class HeapFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0] 
        self.format = build_format(self.schema)
        self.REC_SIZE = struct.calcsize(self.format)
    
    def insert(self, record: dict, additional: dict):
        
        form_record = Record(self.schema, self.format, record)

        with open(self.filename, "r+b") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            
            heapfile.seek(0,2)
            end=heapfile.tell()

            heapfile.seek(4+schema_size)
            deleted = []

            while (heapfile.tell() != end):
                pos = heapfile.tell()
                data = heapfile.read(self.REC_SIZE)

                temp_record = Record.unpack(data, self.format, self.schema)

                if not temp_record.fields["deleted"]:

                    for unique_field in additional["unique"]:
                        if form_record.fields[unique_field] == temp_record.fields[unique_field]:
                            return []
                else:
                    deleted.append(pos)
            
            pos = heapfile.tell()
            if (len(deleted) >= 1):
                heapfile.seek(deleted[0])
                pos = deleted[0]
            
            heapfile.write(form_record.pack())

            return [(form_record.fields,pos)]
    
    def search(self, additional: dict):
        with open(self.filename, "rb") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            heapfile.seek(0,2)
            end=heapfile.tell()
            heapfile.seek(4+schema_size)

            records = []

            while (heapfile.tell() != end):
                data = heapfile.read(self.REC_SIZE)
                record = Record.unpack(data, self.format, self.schema)
                if record.fields[additional["key"]] == additional["value"] and not record.fields["deleted"]:
                    del record.fields["deleted"]
                    records.append(record.fields)
                    if (additional["unique"]):
                        break
                    
        return records

    def range_search(self, additional: dict):
        with open(self.filename, "rb") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            heapfile.seek(0,2)
            end=heapfile.tell()
            heapfile.seek(4+schema_size)

            records = []

            while (heapfile.tell() != end):
                data = heapfile.read(self.REC_SIZE)
                record = Record.unpack(data, self.format, self.schema)
                if additional["min"] <= record.fields[additional["key"]] <= additional["max"] and not record.fields["deleted"]:
                    del record.fields["deleted"]
                    records.append(record.fields)
                    
        return records

    def remove(self, additional: dict):
        with open(self.filename, "r+b") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            heapfile.seek(0,2)
            end=heapfile.tell()
            heapfile.seek(4+schema_size)

            records = []

            while (heapfile.tell() != end):
                pos = heapfile.tell()
                data = heapfile.read(self.REC_SIZE)
                record = Record.unpack(data, self.format, self.schema)
                
                if record.fields[additional["key"]] == additional["value"] and not record.fields["deleted"]:
                    heapfile.seek(pos)
                    record.fields["deleted"] = True
                    heapfile.write(record.pack())
                    
                    del record.fields["deleted"]
                    records.append(record.fields)

                    if (additional["unique"]):
                        break
                    
        return records

    # === ACTUALIZADO: resolver RIDs → registros completos ===
    def search_by_pos(self, pos):
        """
        Si 'pos' es int  -> retorna un solo registro (dict) o None.
        Si 'pos' es list -> compatibilidad con el flujo existente: retorna lista de dicts.
        Ignora registros con 'deleted'=True.
        """
        with open(self.filename, "rb") as f:
            schema_len = struct.unpack("I", f.read(4))[0]
            base = 4 + schema_len

            # Caso 1: una sola posición (int)
            if isinstance(pos, int):
                f.seek(base + pos)
                data = f.read(self.REC_SIZE)
                if not data or len(data) < self.REC_SIZE:
                    return None
                rec = Record.unpack(data, self.format, self.schema)
                if rec.fields.get("deleted"):
                    return None
                rec.fields.pop("deleted", None)
                return rec.fields

            # Caso 2: lista de posiciones/objetos {"pos": ...}
            ret_records = []
            for item in pos:
                p = item["pos"] if isinstance(item, dict) else int(item)
                f.seek(base + p)
                data = f.read(self.REC_SIZE)
                if not data or len(data) < self.REC_SIZE:
                    continue
                rec = Record.unpack(data, self.format, self.schema)
                if rec.fields.get("deleted"):
                    continue
                rec.fields.pop("deleted", None)
                ret_records.append(rec.fields)
            return ret_records

    def delete_by_pos(self, records: list):
        ret_records = []

        with open(self.filename, "r+b") as heapfile:

            for record in records:

                heapfile.seek(record["pos"])
                data = heapfile.read(self.REC_SIZE)
                temp_record = Record.unpack(data,self.format, self.schema)

                temp_record.fields["deleted"] = True
                heapfile.seek(record["pos"])
                heapfile.write(temp_record.pack())

                ret_records.append(temp_record.fields)
            
        return records
