from Methods import get_json, build_format
from Record import Record
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
                        if form_record[unique_field] == temp_record[unique_field]:
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

    def search_by_position(self, positions: list):
        
        with open(self.filename) as heapfile:

            records = []

            for pos in positions:
                heapfile.seek(pos)
                data = heapfile.read(self.REC_SIZE)
                record = Record.unpack(data, self.format, self.schema)

                records.append(record.fields)
        
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