from backend.catalog.catalog import get_json
from backend.core.utils import build_format
from backend.core.record import Record
import struct


class HeapFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0]
        self.format = build_format(self.schema)
        self.REC_SIZE = struct.calcsize(self.format)
        self.read_count = 0
        self.write_count = 0

    def insert(self, record: dict, additional: dict):

        form_record = Record(self.schema, self.format, record)

        with open(self.filename, "r+b") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            self.read_count += 1

            heapfile.seek(0, 2)
            end = heapfile.tell()

            heapfile.seek(4 + schema_size)
            deleted = []

            while (heapfile.tell() != end):
                pos = heapfile.tell()
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1

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
            self.write_count += 1

            return [(form_record.fields, pos)]

    def search(self, additional: dict):
        with open(self.filename, "rb") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            self.read_count += 1

            heapfile.seek(0, 2)
            end = heapfile.tell()
            heapfile.seek(4 + schema_size)

            records = []

            while (heapfile.tell() != end):
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1

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
            self.read_count += 1

            heapfile.seek(0, 2)
            end = heapfile.tell()
            heapfile.seek(4 + schema_size)

            records = []

            while (heapfile.tell() != end):
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1

                record = Record.unpack(data, self.format, self.schema)
                if additional["min"] <= record.fields[additional["key"]] <= additional["max"] and not record.fields[
                    "deleted"]:
                    del record.fields["deleted"]
                    records.append(record.fields)

        return records

    def remove(self, additional: dict):
        with open(self.filename, "r+b") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            self.read_count += 1

            heapfile.seek(0, 2)
            end = heapfile.tell()
            heapfile.seek(4 + schema_size)

            records = []

            while (heapfile.tell() != end):
                pos = heapfile.tell()
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1

                record = Record.unpack(data, self.format, self.schema)

                if record.fields[additional["key"]] == additional["value"] and not record.fields["deleted"]:
                    heapfile.seek(pos)
                    record.fields["deleted"] = True
                    heapfile.write(record.pack())
                    self.write_count += 1

                    del record.fields["deleted"]
                    records.append(record.fields)

                    if (additional["unique"]):
                        break

        return records

    def search_by_pos(self, records: list):

        ret_records = []

        with open(self.filename, "r+b") as heapfile:
            for record in records:
                heapfile.seek(record["pos"])
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1
                temp_record = Record.unpack(data, self.format, self.schema)

                del temp_record.fields["deleted"]
                ret_records.append(temp_record.fields)

        return ret_records

    def delete_by_pos(self, records: list):
        ret_records = []

        with open(self.filename, "r+b") as heapfile:
            for record in records:
                heapfile.seek(record["pos"])
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1
                temp_record = Record.unpack(data, self.format, self.schema)

                temp_record.fields["deleted"] = True
                heapfile.seek(record["pos"])
                heapfile.write(temp_record.pack())
                self.write_count += 1

                ret_records.append(temp_record.fields)

        return ret_records
    
    def get_all(self, get_pos = False):
        with open(self.filename, "rb") as heapfile:
            schema_size = struct.unpack("I", heapfile.read(4))[0]
            self.read_count += 1

            heapfile.seek(0, 2)
            end = heapfile.tell()
            heapfile.seek(4 + schema_size)

            records = []

            while (heapfile.tell() != end):
                pos = heapfile.tell()
                data = heapfile.read(self.REC_SIZE)
                self.read_count += 1

                record = Record.unpack(data, self.format, self.schema)
                if not record.fields["deleted"]:
                    del record.fields["deleted"]

                    if get_pos:
                        records.append((record.fields, pos))
                    else:
                        records.append(record)

        return records