from backend.catalog.catalog import get_json
from backend.core.utils import build_format
from backend.core.record import Record
import struct
import math


class SeqFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0]
        self.format = build_format(self.schema)
        self.REC_SIZE = struct.calcsize(self.format)
        self.read_count = 0
        self.write_count = 0

    def binary_repeated(self, seqfile, value, additional, schema_size, begin, end):
        while begin <= end:
            mid = (begin + end) // 2
            seqfile.seek(4 + schema_size + 4 + (mid * self.REC_SIZE))

            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            temp_record = Record.unpack(data, self.format, self.schema)

            if value == temp_record.fields[additional["key"]]:
                if temp_record.fields["deleted"]:
                    return False
                return True

            elif temp_record.fields[additional["key"]] < value:
                begin = mid + 1
            else:
                end = mid - 1

        return False

    def linear_repeated(self, seqfile, values, elems):
        for _ in range(elems):
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            temp_record = Record.unpack(data, self.format, self.schema)

            for field in values:
                if values[field] == temp_record.fields[field]:
                    return True

        return False

    def sort_and_merge(self, seqfile, additional):
        seqfile.seek(0)

        schema_size = struct.unpack("I", seqfile.read(4))[0]
        self.read_count += 1

        seqfile.seek(0)
        schema_header = seqfile.read(4)
        self.read_count += 1

        schema_data = seqfile.read(schema_size)

        main_elements = struct.unpack("i", seqfile.read(4))[0]
        self.read_count += 1

        main_records = []
        for _ in range(main_elements):
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            temp_record = Record.unpack(data, self.format, self.schema)

            if not temp_record.fields["deleted"]:
                main_records.append(temp_record)

        aux_elements = struct.unpack("i", seqfile.read(4))[0]
        self.read_count += 1

        aux_records = []
        for _ in range(aux_elements):
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            temp_record = Record.unpack(data, self.format, self.schema)
            if not temp_record.fields["deleted"]:
                aux_records.append(temp_record)

        aux_records.sort(key=lambda record: record.fields[additional["key"]])

        merged_records = []
        i, j = 0, 0

        while i < len(main_records) and j < len(aux_records):
            if i < len(main_records) and j < len(aux_records):
                if main_records[i].fields[additional["key"]] < aux_records[j].fields[additional["key"]]:
                    merged_records.append(main_records[i])
                    i += 1
                elif main_records[i].fields[additional["key"]] > aux_records[j].fields[additional["key"]]:
                    merged_records.append(aux_records[j])
                    j += 1
                else:
                    merged_records.append(aux_records[j])
                    i += 1
                    j += 1

        while i < len(main_records):
            merged_records.append(main_records[i])
            i += 1

        while j < len(aux_records):
            merged_records.append(aux_records[j])
            j += 1

        seqfile.seek(0)

        seqfile.truncate()
        self.write_count += 1

        seqfile.write(schema_header)
        seqfile.write(schema_data)

        self.write_count += 2

        seqfile.write(struct.pack("i", len(merged_records)))

        self.write_count += 1

        result_list = []
        for record in merged_records:
            pos = seqfile.tell()
            seqfile.write(record.pack())

            self.write_count += 1

            result_list.append((record.fields, pos))

        seqfile.write(struct.pack("i", 0))
        self.write_count += 1

    def insert(self, record: dict, additional: dict):

        form_record = Record(self.schema, self.format, record)

        with open(self.filename, "r+b") as seqfile:

            schema_size = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            seqfile.seek(0, 2)
            end = seqfile.tell()

            if (end == 4 + schema_size):
                seqfile.seek(4 + schema_size)
                seqfile.write(struct.pack("i", 1))

                self.write_count += 1

                pos = seqfile.tell()
                seqfile.write(form_record.pack())
                seqfile.write(struct.pack("i", 0))

                self.write_count += 2

                return [form_record.fields]

            else:
                seqfile.seek(4 + schema_size)
                main_elements = struct.unpack("i", seqfile.read(4))[0]

                self.read_count += 1

                if len(additional["unique"]) == 1:

                    if self.binary_repeated(seqfile, form_record.fields[additional["key"]], additional, schema_size, 0,
                                            main_elements - 1):
                        return []

                elif len(additional["unique"]) > 1:

                    values = {}
                    for field in additional["unique"]:
                        values[field] = form_record.fields[field]

                    if self.linear_repeated(seqfile, values, main_elements):
                        return []

                seqfile.seek(4 + schema_size + 4 + (main_elements * self.REC_SIZE))
                aux_elements = struct.unpack("i", seqfile.read(4))[0]
                self.read_count += 1

                if len(additional["unique"]) == 0:
                    seqfile.seek(0, 2)
                    seqfile.write(form_record.pack())
                    self.write_count += 1

                    seqfile.seek(4 + schema_size + 4 + (main_elements * self.REC_SIZE))
                    seqfile.write(struct.pack("i", aux_elements + 1))
                    self.write_count += 1


                else:
                    inserted = False

                    for _ in range(aux_elements):

                        pos = seqfile.tell()

                        data = seqfile.read(self.REC_SIZE)
                        self.read_count += 1

                        temp_record = Record.unpack(data, self.format, self.schema)

                        if temp_record.fields["deleted"] and len(additional["unique"]) <= 1:

                            if temp_record.fields[additional["key"]] >= form_record.fields[additional["key"]]:
                                inserted = True
                                seqfile.seek(pos)
                                seqfile.write(form_record.pack())

                                self.write_count += 1

                        else:

                            for field in additional["unique"]:

                                if temp_record.fields[field] == form_record.fields[field]:
                                    return []

                    if not inserted:
                        seqfile.seek(0, 2)
                        seqfile.write(form_record.pack())
                        self.write_count += 1

                        seqfile.seek(4 + schema_size + 4 + (main_elements * self.REC_SIZE))
                        seqfile.write(struct.pack("i", aux_elements + 1))
                        self.write_count += 1

                    max_aux_size = int(math.log2(main_elements)) if main_elements > 0 else 1
                    if aux_elements + 1 > max_aux_size:
                        self.sort_and_merge(seqfile, additional)

                    return [form_record.fields]

    def binary_search(self, seqfile, additional, begin, end, offset):
        while begin <= end:
            mid = (begin + end) // 2
            pos = offset + (mid * self.REC_SIZE)
            seqfile.seek(pos)
            data = seqfile.read(self.REC_SIZE)

            self.read_count += 1

            temp_record = Record.unpack(data, self.format, self.schema)

            if additional["value"] == temp_record.fields[additional["key"]]:
                if temp_record.fields["deleted"]:
                    return []

                del temp_record.fields["deleted"]
                return [temp_record.fields]

            elif temp_record.fields[additional["key"]] < additional["value"]:
                begin = mid + 1
            else:
                end = mid - 1

        return []

    def linear_search(self, seqfile, additional, elems, param=False, same_key=False):

        records = []

        for _ in range(elems):
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            record = Record.unpack(data, self.format, self.schema)

            if record.fields[additional["key"]] > additional["value"] and param and same_key:
                break

            if record.fields[additional["key"]] == additional["value"] and not record.fields["deleted"]:
                del record.fields["deleted"]
                records.append(record.fields)
                if (additional["unique"]):
                    break

        return records

    def search(self, additional: dict, same_key: bool):

        records = []

        with open(self.filename, "r+b") as seqfile:

            schema_size = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            seqfile.seek(0, 2)
            end = seqfile.tell()

            if (end == 4 + schema_size):
                return []

            seqfile.seek(4 + schema_size)
            main_elements = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            if (same_key and additional["unique"]):

                records = self.binary_search(seqfile, additional, 0, main_elements - 1, 4 + schema_size + 4)

                if len(records) == 0:
                    seqfile.seek(4 + schema_size + 4 + (self.REC_SIZE * main_elements))
                    aux_elements = struct.unpack("I", seqfile.read(4))[0]
                    self.read_count += 1

                    records.extend(self.linear_search(seqfile, additional, aux_elements))

            else:
                records = self.linear_search(seqfile, additional, main_elements, True, same_key)
                seqfile.seek(4 + schema_size + 4 + (self.REC_SIZE * main_elements))
                aux_elements = struct.unpack("I", seqfile.read(4))[0]
                self.read_count += 1

                records.extend(self.linear_search(seqfile, additional, aux_elements))

        return records

    def linear_search_by_range(self, seqfile, elems, additional, min_val, max_val, same_key=False):

        records = []

        for _ in range(elems):
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            record = Record.unpack(data, self.format, self.schema)

            if record.fields[additional["key"]] > max_val and same_key:
                break

            if min_val <= record.fields[additional["key"]] <= max_val and not record.fields["deleted"]:
                del record.fields["deleted"]
                records.append(record.fields)

        return records

    def range_search(self, additional: dict, same_key: bool):

        records = []

        with open(self.filename, "r+b") as seqfile:
            schema_size = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            seqfile.seek(0, 2)
            end = seqfile.tell()

            if (end == 4 + schema_size):
                return []

            seqfile.seek(4 + schema_size)
            main_elements = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            records.extend(
                self.linear_search_by_range(seqfile, main_elements, additional, additional["min"], additional["max"],
                                            same_key))

            seqfile.seek(4 + schema_size + 4 + (self.REC_SIZE * main_elements))

            aux_elements = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            records.extend(
                self.linear_search_by_range(seqfile, aux_elements, additional, additional["min"], additional["max"],
                                            same_key))

        return records

    def binary_delete(self, seqfile, additional, begin, end, offset):
        while begin <= end:
            mid = (begin + end) // 2
            pos = offset + (mid * self.REC_SIZE)
            seqfile.seek(pos)
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            temp_record = Record.unpack(data, self.format, self.schema)

            if additional["value"] == temp_record.fields[additional["key"]]:
                if temp_record.fields["deleted"]:
                    return []

                seqfile.seek(pos)
                temp_record.fields["deleted"] = True
                seqfile.write(temp_record.pack())
                self.write_count += 1

                del temp_record.fields["deleted"]
                return [temp_record.fields]

            elif temp_record.fields[additional["key"]] < additional["value"]:
                begin = mid + 1
            else:
                end = mid - 1

        return []

    def linear_delete(self, seqfile, additional, elems, param=False, same_key=False):

        records = []

        for _ in range(elems):
            pos = seqfile.tell()
            data = seqfile.read(self.REC_SIZE)
            self.read_count += 1

            record = Record.unpack(data, self.format, self.schema)

            if record.fields[additional["key"]] > additional["value"] and param and same_key:
                break

            if record.fields[additional["key"]] == additional["value"] and not record.fields["deleted"]:

                seqfile.seek(pos)
                record.fields["deleted"] = True
                seqfile.write(record.pack())
                self.write_count += 1

                del record.fields["deleted"]
                records.append(record.fields)
                if (additional["unique"]):
                    break

        return records

    def remove(self, additional: dict, same_key: bool):
        records = []

        with open(self.filename, "r+b") as seqfile:

            schema_size = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            seqfile.seek(0, 2)
            end = seqfile.tell()

            if (end == 4 + schema_size):
                return []

            seqfile.seek(4 + schema_size)
            main_elements = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            if (same_key and additional["unique"]):

                records = self.binary_delete(seqfile, additional, 0, main_elements - 1, 4 + schema_size + 4)

                if len(records) == 0:
                    seqfile.seek(4 + schema_size + 4 + (self.REC_SIZE * main_elements))
                    aux_elements = struct.unpack("I", seqfile.read(4))[0]
                    self.read_count += 1

                    records.extend(self.linear_delete(seqfile, additional, aux_elements))

            else:
                records = self.linear_delete(seqfile, additional, main_elements, True, same_key)
                seqfile.seek(4 + schema_size + 4 + (self.REC_SIZE * main_elements))
                aux_elements = struct.unpack("I", seqfile.read(4))[0]
                self.read_count += 1

                records.extend(self.linear_delete(seqfile, additional, aux_elements))

        return records
    

    def get_all(self):
        records = []

        with open(self.filename, "r+b") as seqfile:

            schema_size = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            seqfile.seek(0, 2)
            end = seqfile.tell()

            if (end == 4 + schema_size):
                return []

            seqfile.seek(4 + schema_size)
            main_elements = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            for _ in range(main_elements):
                data = seqfile.read(self.REC_SIZE)
                self.read_count += 1

                record = Record.unpack(data, self.format, self.schema)

                if not record.fields["deleted"]:
                    del record.fields["deleted"]
                    records.append(record.fields)
            
            seqfile.seek(4 + schema_size + 4 + (self.REC_SIZE * main_elements))
            aux_elements = struct.unpack("I", seqfile.read(4))[0]
            self.read_count += 1

            for _ in range(aux_elements):
                data = seqfile.read(self.REC_SIZE)
                self.read_count += 1

                record = Record.unpack(data, self.format, self.schema)

                if not record.fields["deleted"]:
                    del record.fields["deleted"]
                    records.append(record.fields)

        return records




            



