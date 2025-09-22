from Methods import get_json, build_format, put_json
from Record import Record
import struct
import os

INDEX_FACTOR = 10
PAGE_FACTOR = 10


class Index:

    def __init__(self, key: int, page: int):
        self.key = key
        self.page = page

    def pack(self, formato)-> bytes:
        return struct.pack(formato, self.key, self.page)

    @staticmethod
    def unpack(data: bytes, formato):
        key, page = struct.unpack(formato, data)
        return Index(key, page)


class IndexPage:

    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, indexes = []):
        self.indexes = indexes

    def pack(self, formato, indexSize):
        header_data = struct.pack("i", len(self.indexes))
        record_data = b''
        for index in self.indexes:
            record_data += index.pack(formato)
        i = len(self.indexes)
        while i < INDEX_FACTOR:
            record_data += b'\x00' * indexSize
            i += 1
        return header_data + record_data

    @staticmethod
    def unpack(data : bytes, formato, indexSize):
        size = struct.unpack(IndexPage.HEADER_FORMAT, data[:IndexPage.HEADER_SIZE])[0]
        offset = IndexPage.HEADER_SIZE
        indexes = []
        for _ in range(size):
            index_data = data[offset: offset + indexSize]
            indexes.append(Index.unpack(index_data, formato))
            offset += indexSize
        return IndexPage(indexes)
    
    @staticmethod
    def getPage(file, page: int, formato, indexSize):
        file.seek((page-1) * (IndexPage.HEADER_SIZE + (indexSize*INDEX_FACTOR)))
        data = file.read(IndexPage.HEADER_SIZE + (indexSize*INDEX_FACTOR))
        return IndexPage.unpack(data, formato, indexSize)

    def __str__(self):
        result = ""
        for index in self.indexes:
            result += f"({index.key})\n"
        return result
    

class DataPage:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, records = [], next_page = -1):
        self.records = records
        self.next_page = next_page

    def pack(self, recordSize):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
        record_data = b''
        for record in self.records:
            record_data += record.pack()
        i = len(self.records)
        while i < PAGE_FACTOR:
            record_data += b'\x00' * recordSize
            i += 1
        return header_data + record_data

    @staticmethod
    def unpack(data : bytes, recordSize, formato, schema):
        size, next_page = struct.unpack(DataPage.HEADER_FORMAT, data[:DataPage.HEADER_SIZE])
        offset = DataPage.HEADER_SIZE
        records = []
        for _ in range(size):
            record_data = data[offset: offset + recordSize]
            records.append(Record.unpack(record_data, formato, schema))
            offset += recordSize
        return DataPage(records, next_page)
    
    @staticmethod
    def getPage(file, page: int, formato, recordSize, schema):
        file.seek((page-1) * (DataPage.HEADER_SIZE + (recordSize*PAGE_FACTOR)))
        data = file.read((DataPage.HEADER_SIZE + (recordSize*PAGE_FACTOR)))
        return DataPage.unpack(data, recordSize, formato, schema)
    
class IsamFile:

    def create_files(self):
        if not os.path.exists(self.delete_filename):
            with open(self.delete_filename, 'w') as _:
                pass 
        
        if not os.path.exists(self.index_filename):
            with open(self.index_filename, 'w') as _:
                pass

    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0] 
        self.format = build_format(self.schema)
        
        self.REC_SIZE = struct.calcsize(self.format)
        
        self.delete_filename = self.filename.replace(".dat", "_delete.dat")
        self.index_filename = self.filename.replace(".dat", "_index.dat")
        self.create_files()
    
    def remove_duplicates(self, records: list, uniques: list):
        if len(uniques) == 0:
            return records
        
        seen = set()
        un_records = []
        
        for record in records:
            unique_values = tuple(record.get(field) for field in uniques)

            if unique_values not in seen:
                seen.add(unique_values)
                un_records.append(record)
        
        return un_records
    
    def insert(self, record: dict, additional: dict):

        indexformat = ""

        for field in self.schema:
            if field["name"] == additional["key"]:
                
                if "length" in field:
                    indexformat += f"{field["length"]}"
                
                indexformat += field["type"]

        indexformat += "i"
        indexsize = struct.calcsize(indexformat)
        page_size = IndexPage.HEADER_SIZE + (indexsize*INDEX_FACTOR)

        if "delete" in record:
            del record["delete"]
        
        with open(self.index_filename, "r+b") as indexfile:

            root = IndexPage.getPage(indexfile, 1, indexformat, indexsize)

            leaf_page = 0

            for i in range(len(root.indexes)):

                if record[additional["key"]] <= root.indexes[i].key:
                    leaf_page = root.indexes[i].page
                    break
            
            if (leaf_page == 0):
                leaf_page = root.indexes[len(root.indexes) - 1].page
                root.indexes[len(root.indexes) - 1].key = record[additional["key"]]
                indexfile.seek(0)
                indexfile.write(root.pack(indexformat, indexsize))

    
            leaf = IndexPage.getPage(indexfile, leaf_page, indexformat, indexsize)

            data_page = 0


            for i in range(len(leaf.indexes)):
                if record[additional["key"]] <= leaf.indexes[i].key:
                    data_page = leaf.indexes[i].page
            
            if (data_page == 0):
                data_page = leaf.indexes[len(leaf.indexes) -1].page
                leaf.indexes[len(leaf.indexes) -1].key = record[additional["key"]]
                indexfile.seek((data_page-1) * page_size)
                indexfile.write(leaf.pack(indexformat, indexsize))

            print(record[additional["key"]], data_page, end = "\n")
                    

                    
    def build(self, records: list, additional: dict):

        records = self.remove_duplicates(records, additional["unique"])

        if len(records) == 0 or os.path.getsize(self.index_filename) > 0:
            return []
                    
        records.sort(key=lambda x: x.get(additional["key"], 0))
        
        all_keys = [record.get(additional["key"], 0) for record in records]

        middle_idx = len(all_keys) // 2
        left_keys = all_keys[:middle_idx]
        right_keys = all_keys[middle_idx:]
        
        root = all_keys[middle_idx]

        indexformat = ""

        for field in self.schema:
            if field["name"] == additional["key"]:
                
                if "length" in field:
                    indexformat += f"{field["length"]}"
                
                indexformat += field["type"]

        indexformat += "i"
        indexsize = struct.calcsize(indexformat)

        root_index = IndexPage([Index(root, 2), Index(all_keys[len(all_keys)-1], 3)])
        left_index = IndexPage([Index(root, 1)])
        right_index = IndexPage([Index(all_keys[len(all_keys)-1], 2)])

        with open(self.index_filename, "r+b") as indexfile:
            indexfile.write(root_index.pack(indexformat, indexsize))
            indexfile.write(left_index.pack(indexformat, indexsize))
            indexfile.write(right_index.pack(indexformat, indexsize))
        
        with open(self.filename, "r+b") as datafile:
            schema_size = struct.unpack("I", datafile.read(4))[0]

            datafile.seek(4+schema_size)

            Page = DataPage([])

            datafile.write(Page.pack(self.REC_SIZE))
            datafile.write(Page.pack(self.REC_SIZE))
        
        for record in records:
            self.insert(record, additional)








    