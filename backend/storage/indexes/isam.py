from backend.catalog.catalog import get_json
from backend.core.utils import build_format
from backend.core.record import Record
import struct
import os

INDEX_FACTOR = 2
PAGE_FACTOR = 4

class Index:

    def __init__(self, key: int, page: int):
        self.key = key
        self.page = page

    def pack(self, formato) -> bytes:
        return struct.pack(formato, self.key, self.page)

    @staticmethod
    def unpack(data: bytes, formato):
        if len(data) < struct.calcsize(formato):
            raise ValueError(f"Insufficient data: expected {struct.calcsize(formato)} bytes, got {len(data)} bytes")
        key, page = struct.unpack(formato, data)
        return Index(key, page)


class IndexPage:
    HEADER_FORMAT = 'i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, indexes=[]):
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
    def unpack(data: bytes, formato, indexSize):
        size = struct.unpack(IndexPage.HEADER_FORMAT, data[:IndexPage.HEADER_SIZE])[0]
        offset = IndexPage.HEADER_SIZE
        indexes = []
        for _ in range(size):
            index_data = data[offset: offset + indexSize]
            # Skip if we encounter null padding
            if index_data == b'\x00' * indexSize:
                break
            indexes.append(Index.unpack(index_data, formato))
            offset += indexSize
        return IndexPage(indexes)

    @staticmethod
    def getPage(file, page: int, formato, indexSize):
        file.seek((page - 1) * (IndexPage.HEADER_SIZE + (indexSize * INDEX_FACTOR)))
        data = file.read(IndexPage.HEADER_SIZE + (indexSize * INDEX_FACTOR))
        return IndexPage.unpack(data, formato, indexSize)

    @staticmethod
    def getTotalPages(file, page_size):
        file.seek(0, 2)
        return file.tell() // page_size

    def __str__(self):
        result = ""
        for index in self.indexes:
            result += f"({index.key} + {index.page})\n"
        return result


class DataPage:
    HEADER_FORMAT = 'ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, records=[], next_page=-1):
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
    def unpack(data: bytes, recordSize, formato, schema):
        size, next_page = struct.unpack(DataPage.HEADER_FORMAT, data[:DataPage.HEADER_SIZE])
        offset = DataPage.HEADER_SIZE
        records = []
        for _ in range(size):
            record_data = data[offset: offset + recordSize]
            records.append(Record.unpack(record_data, formato, schema))
            offset += recordSize
        return DataPage(records, next_page)

    @staticmethod
    def getPage(file, page: int, formato, recordSize, schema, schema_size):
        page_size = DataPage.HEADER_SIZE + (recordSize * PAGE_FACTOR)
        file.seek(4 + schema_size + (page - 1) * page_size)
        data = file.read(page_size)
        return DataPage.unpack(data, recordSize, formato, schema)

    @staticmethod
    def getTotalPages(file, page_size, schema_size):
        file.seek(0, 2)
        end = file.tell()
        end -= 4 + schema_size
        return end // page_size

    def __str__(self):
        result = ""
        for record in self.records:
            result += f"({record.fields})\n"
        return result


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

        self.read_count=0
        self.write_count=0

    def get_metrics(self, additional):
        """
        Construye el formato de struct para entradas del índice: [key][page_ptr]
        - key: codificada según tipo (i, f, Ns, ?)
        - page_ptr: int ('i')
        Devuelve: (indexformat, indexsize, index_page_size, data_page_size)
        """
        # 1) localizar columna clave
        key_name = additional.get("key")
        key_field = None
        for f in self.schema:
            if f.get("name") == key_name:
                key_field = f
                break
        if key_field is None:
            raise KeyError(f"ISAM: clave '{key_name}' no está en schema")

        # 2) mapear tipo humano -> código struct
        t = str(key_field.get("type") or "").lower()
        indexformat = ""
        if t in ("i", "int", "integer", "smallint", "bigint", "serial"):
            indexformat = "i"
        elif t in ("f", "float", "real", "double", "double precision"):
            indexformat = "f"
        elif t in ("s", "char", "varchar", "string"):
            ln = int(key_field.get("length", 32))
            indexformat = f"{ln}s"
        elif t in ("?", "bool", "boolean"):
            indexformat = "?"
        else:
            raise ValueError(f"ISAM: tipo de clave no soportado: {t!r}")

        # 3) agregar puntero a página (entero)
        indexformat += "i"

        # 4) tamaños
        indexsize = struct.calcsize(indexformat)
        index_page_size = IndexPage.HEADER_SIZE + (indexsize * INDEX_FACTOR)
        data_page_size = DataPage.HEADER_SIZE + (self.REC_SIZE * PAGE_FACTOR)

        return indexformat, indexsize, index_page_size, data_page_size

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

    def check_duplicates(self, records, record, additional):
        if len(additional["unique"]) == 0:
            return False

        for temp in records:

            for field in additional["unique"]:

                if temp.fields[field] == record.fields[field]:
                    return True

        return False

    def additional_check(self, record, additional):
        if len(additional["unique"]) == 1:
            return False

        with open(self.filename, "r+b") as mainfile:
            mainfile.seek(0, 2)
            end = mainfile.tell()

            mainfile.seek(0)
            schema_size = struct.unpack("I", mainfile.read(4))[0]
            self.read_count+=1

            mainfile.seek(4 + schema_size)

            i = 1
            while (end != mainfile.tell()):
                page = DataPage.getPage(mainfile, i, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

                for temp_record in page.records:

                    for key in additional["unique"]:

                        if record[key] == temp_record.fields[key]:
                            return True

                i += 1

            return False

    def insert_on_page(self, record, additional, mainfile, indexfile, leaf, root, leaf_number, page_number, indexformat,
                       indexsize, index_page_size, data_page_size):

        mainfile.seek(0)
        schema_size = struct.unpack("I", mainfile.read(4))[0]
        self.read_count+=1

        page = DataPage.getPage(mainfile, page_number, self.format, self.REC_SIZE, self.schema, schema_size)
        self.read_count+=1

        if len(page.records) < PAGE_FACTOR:

            if self.check_duplicates(page.records, Record(self.schema, self.format, record), additional):
                return []

            page.records.append(Record(self.schema, self.format, record))
            page.records.sort(key=lambda x: x.fields[additional["key"]])
            mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
            mainfile.write(page.pack(self.REC_SIZE))
            self.write_count+=1

        else:

            if self.check_duplicates(page.records, Record(self.schema, self.format, record), additional):
                return []

            if not (len(leaf.indexes) == INDEX_FACTOR and len(root.indexes) == INDEX_FACTOR):
                page.records.append(Record(self.schema, self.format, record))
                page.records.sort(key=lambda x: x.fields[additional["key"]])

                middle = len(page.records) // 2
                left_records = page.records[:middle]
                right_records = page.records[middle:]

                page.records = right_records
                mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
                mainfile.write(page.pack(self.REC_SIZE))
                self.write_count+=1

                new_page = DataPage(left_records)
                mainfile.seek(0, 2)
                mainfile.write(new_page.pack(self.REC_SIZE))
                self.write_count+=1

                new_key = left_records[-1].fields[additional["key"]]
                new_index = Index(new_key, DataPage.getTotalPages(mainfile, data_page_size, schema_size))

                leaf.indexes.append(new_index)
                leaf.indexes.sort(key=lambda x: x.key)

                indexfile.seek((leaf_number - 1) * index_page_size)
                indexfile.write(leaf.pack(indexformat, indexsize))

                self.write_count+=1

                if (len(leaf.indexes) > INDEX_FACTOR):
                    middle = len(leaf.indexes) // 2
                    left_indexes = leaf.indexes[:middle]
                    right_indexes = leaf.indexes[middle:]

                    leaf.indexes = right_indexes
                    indexfile.seek((leaf_number - 1) * index_page_size)
                    indexfile.write(leaf.pack(indexformat, indexsize))
                    self.write_count+=1

                    new_index_page = IndexPage(left_indexes)
                    indexfile.seek(0, 2)
                    new_page_number = IndexPage.getTotalPages(indexfile, index_page_size) + 1
                    indexfile.write(new_index_page.pack(indexformat, indexsize))
                    self.write_count+=1

                    new_key = left_indexes[-1].key
                    new_index = Index(new_key, new_page_number)

                    root.indexes.append(new_index)
                    root.indexes.sort(key=lambda x: x.key)

                    indexfile.seek(0)
                    indexfile.write(root.pack(indexformat, indexsize))
                    self.write_count+=1

            else:

                record = Record(self.schema, self.format, record)

                while True:

                    if self.check_duplicates(page.records, record, additional):
                        return []

                    page.records.append(Record(self.schema, self.format, record.fields))
                    page.records.sort(key=lambda x: x.fields[additional["key"]])

                    if (len(page.records) > PAGE_FACTOR):
                        record = page.records.pop()

                        mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
                        mainfile.write(page.pack(self.REC_SIZE))
                        self.write_count+=1

                        if (page.next_page != -1):
                            page_number = page.next_page
                            page = DataPage.getPage(mainfile, page_number, self.format, self.REC_SIZE, self.schema,
                                                    schema_size)
                            self.read_count+=1

                        else:

                            new_page = DataPage([Record(self.schema, self.format, record.fields)])
                            page.next_page = DataPage.getTotalPages(mainfile, data_page_size, schema_size) + 1

                            mainfile.seek(0, 2)
                            mainfile.write(new_page.pack(self.REC_SIZE))
                            self.write_count+=1

                            mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
                            mainfile.write(page.pack(self.REC_SIZE))
                            self.write_count+=1

                            break

                    else:
                        mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
                        mainfile.write(page.pack(self.REC_SIZE))
                        self.write_count+=1
                        break

        return [record]

    def insert(self, record: dict, additional: dict):
        if self.additional_check(record, additional):
            return []

        indexformat, indexsize, index_page_size, data_page_size = self.get_metrics(additional)

        if "delete" in record:
            del record["delete"]

        with open(self.index_filename, "r+b") as indexfile:

            root = IndexPage.getPage(indexfile, 1, indexformat, indexsize)
            self.read_count+=1

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
                self.write_count+=1

            leaf = IndexPage.getPage(indexfile, leaf_page, indexformat, indexsize)
            self.read_count+=1

            data_page = 0

            for i in range(len(leaf.indexes)):
                if record[additional["key"]] <= leaf.indexes[i].key:
                    data_page = leaf.indexes[i].page

            if (data_page == 0):
                data_page = leaf.indexes[len(leaf.indexes) - 1].page
                leaf.indexes[len(leaf.indexes) - 1].key = record[additional["key"]]
                indexfile.seek((leaf_page - 1) * index_page_size)
                indexfile.write(leaf.pack(indexformat, indexsize))
                self.write_count+=1

            with open(self.filename, "r+b") as mainfile:
                return self.insert_on_page(record, additional, mainfile, indexfile, leaf, root, leaf_page, data_page,
                                           indexformat, indexsize, index_page_size, data_page_size)

    def build(self, records: list, additional: dict):
        records = self.remove_duplicates(records, additional["unique"])

        if len(records) == 0 or os.path.getsize(self.index_filename) > 0:
            return []

        records.sort(key=lambda x: x.get(additional["key"], 0))

        all_keys = [record.get(additional["key"], 0) for record in records]

        middle_idx = len(all_keys) // 2
        root = all_keys[middle_idx]

        indexformat, indexsize, _, _ = self.get_metrics(additional)

        root_index = IndexPage([Index(root, 2), Index(all_keys[len(all_keys) - 1], 3)])
        left_index = IndexPage([Index(root, 1)])
        right_index = IndexPage([Index(all_keys[len(all_keys) - 1], 2)])

        with open(self.index_filename, "r+b") as indexfile:
            indexfile.write(root_index.pack(indexformat, indexsize))
            indexfile.write(left_index.pack(indexformat, indexsize))
            indexfile.write(right_index.pack(indexformat, indexsize))

            self.write_count+=3

        with open(self.filename, "r+b") as datafile:
            schema_size = struct.unpack("I", datafile.read(4))[0]
            self.read_count+=1

            datafile.seek(4 + schema_size)

            Page = DataPage([])

            datafile.write(Page.pack(self.REC_SIZE))
            datafile.write(Page.pack(self.REC_SIZE))

            self.write_count+=2

        ret_records = []

        for record in records:
            ret_records.extend(self.insert(record, additional))

        return ret_records

    def search_on_page(self, additional, mainfile, page_number):
        mainfile.seek(0)
        schema_size = struct.unpack("I", mainfile.read(4))[0]
        self.read_count+=1

        page = DataPage.getPage(mainfile, page_number, self.format, self.REC_SIZE, self.schema, schema_size)
        self.read_count+=1

        records = []

        while True:

            if len(page.records) == 0:
                break

            for record in page.records:

                if record.fields[additional["key"]] > additional["value"]:
                    return records

                if record.fields[additional["key"]] == additional["value"]:
                    del record.fields["deleted"]
                    records.append(record.fields)

                    if additional["unique"]:
                        return records

            if (page.next_page != -1):
                page = DataPage.getPage(mainfile, page.next_page, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

            else:
                break

        return records

    def search_by_index(self, additional: dict):
        indexformat, indexsize, _, _ = self.get_metrics(additional)

        with open(self.index_filename, "r+b") as indexfile:
            root = IndexPage.getPage(indexfile, 1, indexformat, indexsize)
            self.read_count += 1

            # localizar leaf
            leaf_page = 0
            for i in range(len(root.indexes)):
                if additional["value"] <= root.indexes[i].key:
                    leaf_page = root.indexes[i].page
                    break
            if leaf_page == 0:
                leaf_page = root.indexes[-1].page

            leaf = IndexPage.getPage(indexfile, leaf_page, indexformat, indexsize)
            self.read_count += 1

            # localizar data_page candidata dentro del leaf y recordar índice
            data_page = 0
            leaf_idx = -1
            for i in range(len(leaf.indexes)):
                if additional["value"] <= leaf.indexes[i].key:
                    data_page = leaf.indexes[i].page
                    leaf_idx = i
                    break
            if data_page == 0:
                data_page = leaf.indexes[-1].page
                leaf_idx = len(leaf.indexes) - 1

            # intento normal
            with open(self.filename, "r+b") as mainfile:
                out = self.search_on_page(additional, mainfile, data_page)
                if out:
                    return out

                # 🔁 Fallback robusto: busca en páginas vecinas del MISMO leaf
                # (hacia atrás y hacia adelante). Corrige desalineaciones límite.
                # hacia atrás
                for j in range(leaf_idx - 1, -1, -1):
                    out = self.search_on_page(additional, mainfile, leaf.indexes[j].page)
                    if out:
                        return out
                # hacia adelante
                for j in range(leaf_idx + 1, len(leaf.indexes)):
                    out = self.search_on_page(additional, mainfile, leaf.indexes[j].page)
                    if out:
                        return out

        return []

    def search_seq(self, additional: dict):

        with open(self.filename, "r+b") as mainfile:
            mainfile.seek(0, 2)
            end = mainfile.tell()

            mainfile.seek(0)
            schema_size = struct.unpack("I", mainfile.read(4))[0]
            self.read_count+=1

            mainfile.seek(4 + schema_size)

            records = []

            i = 1
            while (end != mainfile.tell()):
                page = DataPage.getPage(mainfile, i, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

                for record in page.records:

                    if record.fields[additional["key"]] == additional["value"]:

                        del record.fields["deleted"]
                        records.append(record.fields)

                        if additional["unique"]:
                            return records
                i += 1

            return records

    def search(self, additional: dict, same_key: bool):
        if (same_key):
            return self.search_by_index(additional)
        else:
            return self.search_seq(additional)

    def search_on_page_range(self, additional, mainfile, page_number):
        mainfile.seek(0)
        schema_size = struct.unpack("I", mainfile.read(4))[0]
        self.read_count+=1

        page = DataPage.getPage(mainfile, page_number, self.format, self.REC_SIZE, self.schema, schema_size)
        self.read_count+=1

        records = []

        while True:

            if len(page.records) == 0:
                break

            for record in page.records:

                if additional["min"] <= record.fields[additional["key"]] <= additional["max"]:
                    del record.fields["deleted"]
                    records.append(record.fields)

                if record.fields[additional["key"]] >= additional["max"]:
                    return records

            if (page.next_page != -1):
                page = DataPage.getPage(mainfile, page.next_page, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

            else:
                break

        return records

    def search_range_by_index(self, additional):

        indexformat, indexsize, _, _ = self.get_metrics(additional)

        records = []

        with open(self.index_filename, "r+b") as indexfile:

            with open(self.filename, "r+b") as mainfile:

                root = IndexPage.getPage(indexfile, 1, indexformat, indexsize)
                self.read_count+=1

                for i in range(len(root.indexes)):

                    if additional["min"] <= root.indexes[i].key or additional["max"] <= root.indexes[i].key:

                        leaf_page = root.indexes[i].page

                        leaf = IndexPage.getPage(indexfile, leaf_page, indexformat, indexsize)
                        self.read_count+=1

                        for j in range(len(leaf.indexes)):

                            if additional["min"] <= leaf.indexes[j].key or additional["max"] <= leaf.indexes[j].key:
                                data_page = leaf.indexes[j].page

                                records.extend(self.search_on_page_range(additional, mainfile, data_page))

                            if additional["min"] <= leaf.indexes[j].key and additional["max"] <= leaf.indexes[j].key:
                                break

                    if additional["min"] <= root.indexes[i].key and additional["max"] <= root.indexes[i].key:
                        break

        return records

    def search_range_seq(self, additional):

        with open(self.filename, "r+b") as mainfile:
            mainfile.seek(0, 2)
            end = mainfile.tell()

            mainfile.seek(0)
            schema_size = struct.unpack("I", mainfile.read(4))[0]
            self.read_count+=1

            mainfile.seek(4 + schema_size)

            records = []

            i = 1
            while (end != mainfile.tell()):
                page = DataPage.getPage(mainfile, i, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

                for record in page.records:

                    if additional["min"] <= record.fields[additional["key"]] <= additional["max"]:
                        del record.fields["deleted"]
                        records.append(record.fields)

                i += 1

            return records

    def range_search(self, additional: dict, same_key: bool):

        if (same_key):
            return self.search_range_by_index(additional)
        else:
            return self.search_range_seq(additional)

    def rest_list(self, additional, mainfile, page: DataPage, page_number, data_page_size, schema_size):

        repeat = False

        while True:

            if (page.next_page == -1):
                mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
                mainfile.write(page.pack(self.REC_SIZE))
                self.write_count+=1
                break

            next_page = DataPage.getPage(mainfile, page.next_page, self.format, self.REC_SIZE, self.schema, schema_size)
            self.read_count+=1

            if len(next_page.records) == 0:
                mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
                mainfile.write(page.pack(self.REC_SIZE))
                self.write_count+=1
                break

            if next_page.records[0].fields[additional["key"]] == additional["value"]:
                repeat = True

            temp_record = next_page.records.pop(0)

            page.records.append(temp_record)

            mainfile.seek(4 + schema_size + (page_number - 1) * data_page_size)
            mainfile.write(page.pack(self.REC_SIZE))
            self.write_count+=1

            page_number = page.next_page
            page = next_page

        return repeat

    def remove_seq(self, additional: dict):

        _, _, _, data_page_size = self.get_metrics(additional)

        with open(self.filename, "r+b") as mainfile:
            mainfile.seek(0, 2)
            end = mainfile.tell()

            mainfile.seek(0)
            schema_size = struct.unpack("I", mainfile.read(4))[0]
            self.read_count+=1

            mainfile.seek(4 + schema_size)

            records = []

            i = 1
            while (end != mainfile.tell()):
                repeat = False

                page = DataPage.getPage(mainfile, i, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

                for j in range(len(page.records)):

                    if page.records[j].fields[additional["key"]] == additional["value"]:
                        temp_record = page.records.pop(j)
                        records.append(temp_record.fields)
                        repeat = self.rest_list(additional, mainfile, page, i, data_page_size, schema_size)
                        break

                if not repeat:
                    i += 1

                if additional["unique"]:
                    break

            return records

    def remove_index(self, additional: dict):
        indexformat, indexsize, _, data_page_size = self.get_metrics(additional)
        records = []

        with open(self.index_filename, "r+b") as indexfile:

            root = IndexPage.getPage(indexfile, 1, indexformat, indexsize)
            self.read_count+=1

            leaf_page = 0

            for i in range(len(root.indexes)):

                if additional["value"] <= root.indexes[i].key:
                    leaf_page = root.indexes[i].page
                    break

            if (leaf_page == 0):
                return records

            leaf = IndexPage.getPage(indexfile, leaf_page, indexformat, indexsize)
            self.read_count+=1

            data_page = 0

            for i in range(len(leaf.indexes)):
                if additional["value"] <= leaf.indexes[i].key:
                    data_page = leaf.indexes[i].page
                    break

            if (data_page == 0):
                return records

            with open(self.filename, "r+b") as mainfile:

                mainfile.seek(0)
                schema_size = struct.unpack("I", mainfile.read(4))[0]

                page = DataPage.getPage(mainfile, data_page, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

                page_number = data_page

                while True:

                    found = False

                    if len(page.records) == 0:
                        break

                    for j in range(len(page.records)):

                        if page.records[j].fields[additional["key"]] == additional["value"]:
                            found = True
                            temp_record = page.records.pop(j)
                            records.append(temp_record.fields)
                            self.rest_list(additional, mainfile, page, page_number, data_page_size, schema_size)
                            break

                    if found:
                        break

                    if page.next_page == -1:
                        break

                    page_number = page.next_page
                    page = DataPage.getPage(mainfile, page_number, self.format, self.REC_SIZE, self.schema, schema_size)
                    self.read_count+=1

        return records

    def remove(self, additional: dict, same_key: bool):
        if (same_key):
            return self.remove_index(additional)
        else:
            return self.remove_seq(additional)
    

    def get_all_on_page(self, mainfile, page_number):
        mainfile.seek(0)
        schema_size = struct.unpack("I", mainfile.read(4))[0]
        self.read_count+=1

        page = DataPage.getPage(mainfile, page_number, self.format, self.REC_SIZE, self.schema, schema_size)
        self.read_count+=1

        records = []

        while True:

            if len(page.records) == 0:
                break

            for record in page.records:

                del record.fields["deleted"]
                records.append(record.fields)

            if (page.next_page != -1):
                page = DataPage.getPage(mainfile, page.next_page, self.format, self.REC_SIZE, self.schema, schema_size)
                self.read_count+=1

            else:
                break

        return records


    def get_all(self):

        records = []

        with open(self.filename, "r+b") as mainfile:

            mainfile.seek(0)
            schema_size = struct.unpack("I", mainfile.read(4))[0]
            self.read_count+=1
            
            page_size = DataPage.HEADER_SIZE + (self.REC_SIZE * PAGE_FACTOR)

            number_pages = DataPage.getTotalPages(mainfile,page_size,schema_size)

            for page_number in range(1, number_pages+1):
                records.extend(self.get_all_on_page(mainfile, page_number))
        
        return records