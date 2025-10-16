import struct
from backend.core.record import Record
from backend.catalog.catalog import get_json
from backend.core.utils import build_format


BUCKET_SIZE = 3
HEADER_FORMAT = 'ii'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class Bucket:

    def __init__(self, local_depth=1):
        self.records = []
        self.local_depth = local_depth

    def is_full(self):
        return len(self.records) >= BUCKET_SIZE

    def put(self, record):
        if not self.is_full():
            self.records.append(record)
            return True
        return False

    def find(self, key_value, key_name):
        for rec in self.records:
            if rec.fields[key_name] == key_value:
                return rec
        return None

    def remove(self, key_value, key_name):
        for i, rec in enumerate(self.records):
            if rec.fields[key_name] == key_value:
                return self.records.pop(i)
        return None

    def pack(self, record_size, record_format, schema):
        packed_records = b''.join(rec.pack() for rec in self.records)
        padding = b'\x00' * (BUCKET_SIZE * record_size - len(packed_records))
        return packed_records + padding

    @classmethod
    def unpack(cls, data, local_depth, record_size, record_format, schema):
        bucket = cls(local_depth)
        for i in range(BUCKET_SIZE):
            offset = i * record_size
            chunk = data[offset: offset + record_size]
            if chunk == b'\x00' * record_size:
                break
            try:
                record = Record.unpack(chunk, record_format, schema)
                if not record.fields.get("deleted", False):
                    bucket.records.append(record)
            except struct.error:
                break
        return bucket


class ExtendibleHashingFile:

    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0]
        self.format = build_format(self.schema)
        self.record_size = struct.calcsize(self.format)
        self.bucket_disk_size = self.record_size * BUCKET_SIZE
        self.global_depth = 1
        self.directory = [0, 1]
        self.next_page_idx = 2
        self.read_count = 0
        self.write_count = 0


        self._load_or_init()

    def _hash(self, key):
        if isinstance(key, str):
            return sum(ord(c) for c in key)
        return int(key)

    def _get_bucket_idx(self, key):
        h = self._hash(key)
        return h & ((1 << self.global_depth) - 1)

    def _load_or_init(self):
        try:
            with open(self.filename, 'r+b') as f:
                header = f.read(8)
                if not header:
                    self._init_file()
                    return
                self.read_count += 1
                self.global_depth, self.next_page_idx = struct.unpack('ii', header)
                dir_size = 1 << self.global_depth
                dir_bytes = f.read(dir_size * 4)
                self.directory = list(struct.unpack(f'{dir_size}i', dir_bytes))
        except FileNotFoundError:
            self._init_file()

    def _init_file(self):
        self.global_depth = 1
        self.directory = [0, 1]
        self.next_page_idx = 2
        with open(self.filename, 'wb') as f:
            f.write(struct.pack('ii', self.global_depth, self.next_page_idx))
            f.write(struct.pack(f'{len(self.directory)}i', *self.directory))
            f.seek(self._get_page_offset(0))
            f.write(b'\x00' * self.bucket_disk_size)
            f.write(b'\x00' * self.bucket_disk_size)
            self.write_count += 4
        self._write_bucket_depth(0, 1)
        self._write_bucket_depth(1, 1)

    def _get_page_offset(self, page_idx):
        dir_size_on_disk = 1 << self.global_depth
        return 8 + (dir_size_on_disk * 4) + (page_idx * (self.bucket_disk_size + 4))

    def _read_bucket(self, page_idx):
        with open(self.filename, 'rb') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            depth_bytes = f.read(4)
            local_depth = struct.unpack('i', depth_bytes)[0]
            data = f.read(self.bucket_disk_size)
            self.read_count += 1
            return Bucket.unpack(data, local_depth, self.record_size, self.format, self.schema)

    def _write_bucket(self, page_idx, bucket):
        with open(self.filename, 'r+b') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            f.write(struct.pack('i', bucket.local_depth))
            f.write(bucket.pack(self.record_size, self.format, self.schema))
            self.write_count += 1

    def _write_bucket_depth(self, page_idx, depth):
        with open(self.filename, 'r+b') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            f.write(struct.pack('i', depth))
            self.write_count += 1

    def _write_directory(self):
        with open(self.filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('ii', self.global_depth, self.next_page_idx))
            f.write(struct.pack(f'{len(self.directory)}i', *self.directory))
            self.write_count += 1

    def find(self, key_value, key_name="id"):
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        bucket = self._read_bucket(page_idx)
        record = bucket.find(key_value, key_name)
        if record:
            clean_fields = {k: v for k, v in record.fields.items() if k not in ['deleted']}
            return clean_fields
        return None

    def insert(self, record_data, key_name="id"):
        key_value = record_data[key_name]
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        bucket = self._read_bucket(page_idx)

        if not bucket.is_full():
            record = Record(self.schema, self.format, record_data)
            bucket.put(record)
            self._write_bucket(page_idx, bucket)
            return record_data

        self._split(dir_idx, page_idx, bucket)

        return self.insert(record_data, key_name)

    def _split(self, dir_idx, page_idx, bucket):
        if bucket.local_depth == self.global_depth:
            self.directory *= 2
            self.global_depth += 1

        new_page_idx = self.next_page_idx
        self.next_page_idx += 1

        bucket.local_depth += 1
        new_bucket = Bucket(local_depth=bucket.local_depth)

        old_records = bucket.records[:]
        bucket.records = []

        prefix = dir_idx & ((1 << (bucket.local_depth - 1)) - 1)

        for i in range(len(self.directory)):
            if (i & ((1 << (bucket.local_depth - 1)) - 1)) == prefix:
                if (i >> (bucket.local_depth - 1)) & 1:
                    self.directory[i] = new_page_idx

        self._write_bucket(page_idx, bucket)
        self._write_bucket(new_page_idx, new_bucket)
        self._write_directory()

        for rec in old_records:
            self.insert(rec.fields)

    def remove(self, key_value, key_name="id"):
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        bucket = self._read_bucket(page_idx)

        removed_record = bucket.remove(key_value, key_name)
        if removed_record:
            self._write_bucket(page_idx, bucket)
            return removed_record.fields
        return None

    def get_all_records(self):
        all_records = []
        seen_pages = set()
        for page_idx in self.directory:
            if page_idx not in seen_pages:
                bucket = self._read_bucket(page_idx)
                for record in bucket.records:
                    all_records.append(record.fields)
                seen_pages.add(page_idx)
        return all_records




