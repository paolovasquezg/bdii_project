import struct
from backend.core.record import Record
from backend.catalog.catalog import get_json
from backend.core.utils import build_format
 
BUCKET_SIZE = 5
HEADER_FORMAT = 'ii'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
FILE_HEADER_FORMAT_OLD = 'ii'
FILE_HEADER_FORMAT = 'iii'
FILE_HEADER_SIZE_OLD = struct.calcsize(FILE_HEADER_FORMAT_OLD) 
FILE_HEADER_SIZE = struct.calcsize(FILE_HEADER_FORMAT) 
INITIAL_MAX_CHAIN = 2
MAX_GLOBAL_DEPTH = 20 

# ================== Bucket ==================
class Bucket:
    def __init__(self, local_depth=1, overflow_page=-1):
        self.records = []
        self.local_depth = local_depth
        self.overflow_page = overflow_page

    def is_full(self):
        return len(self.records) >= BUCKET_SIZE

    def put(self, record):
        if not self.is_full():
            self.records.append(record)
            return True
        return False

    def find(self, key_value, key_name):
        return [rec for rec in self.records if rec.fields[key_name] == key_value]

    def remove(self, key_value, key_name):
        removed = []
        i = 0
        while i < len(self.records):
            if self.records[i].fields[key_name] == key_value:
                removed.append(self.records.pop(i))
            else:
                i += 1
        return removed if removed else None

    def pack(self, record_size, record_format, schema):
        packed = b''.join(rec.pack() for rec in self.records[:BUCKET_SIZE])
        padding = b'\x00' * (BUCKET_SIZE * record_size - len(packed))
        return packed + padding

    @classmethod
    def unpack(cls, data, local_depth, overflow_page, record_size, record_format, schema):
        bucket = cls(local_depth, overflow_page)
        for i in range(BUCKET_SIZE):
            off = i * record_size
            chunk = data[off: off + record_size]
            if chunk == b'\x00' * record_size:
                continue
            try:
                record = Record.unpack(chunk, record_format, schema)
                if hasattr(record, 'fields') and isinstance(record.fields, dict):
                    if not record.fields.get("deleted", False):
                        bucket.records.append(record)
            except Exception:
                continue
        return bucket


# ================== Hash Extensible ==================
class ExtendibleHashingFile:

    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0]
        self.format = build_format(self.schema)
        self.record_size = struct.calcsize(self.format)
        self.bucket_disk_size = self.record_size * BUCKET_SIZE

        self._json_offset_cached = None
        self._pages_base_offset_cached = None

        # Estado en memoria
        self.global_depth = 1
        self.directory = [0, 1]
        self.next_page_idx = 2
        self.dir_capacity = 2 
        self._file_header_size = FILE_HEADER_SIZE
        self.read_count = 0
        self.write_count = 0

        self.key_name = None
        self._load_or_init()

    def _json_offset(self) -> int:
        if self._json_offset_cached is not None:
            return self._json_offset_cached
        try:
            with open(self.filename, 'rb') as f:
                b = f.read(4)
                self.read_count += 1
                if not b or len(b) < 4:
                    self._json_offset_cached = 0
                    return 0
                size = struct.unpack('I', b)[0]
                self._json_offset_cached = 4 + size
                return self._json_offset_cached
        except FileNotFoundError:
            self._json_offset_cached = 0
            return 0

    def _pages_base_offset(self) -> int:
        # Región de páginas empieza después del file header + directorio (capacidad actual)
        if self._pages_base_offset_cached is not None:
            return self._pages_base_offset_cached
        base = self._json_offset() + self._file_header_size + (self.dir_capacity * 4)
        self._pages_base_offset_cached = base
        return base

    def _get_page_offset(self, page_idx):
        return self._pages_base_offset() + page_idx * (HEADER_SIZE + self.bucket_disk_size)

    def _hash(self, key):
        if isinstance(key, str):
            return sum(ord(c) for c in key)
        return int(key)

    def _get_bucket_idx(self, key):
        h = self._hash(key)
        return h & ((1 << self.global_depth) - 1)

    def _max_chain_length(self):
        return INITIAL_MAX_CHAIN + self.global_depth

    def _load_or_init(self):
        try:
            with open(self.filename, 'r+b') as f:
                off = self._json_offset()
                f.seek(off)
                # Intentar leer header nuevo primero
                header = f.read(FILE_HEADER_SIZE)
                if header and header != b'\x00' * FILE_HEADER_SIZE:
                    self.read_count += 1
                    gd, npi, cap = struct.unpack(FILE_HEADER_FORMAT, header)
                    self.global_depth, self.next_page_idx = gd, npi
                    self.dir_capacity = max(2, cap)
                    self._file_header_size = FILE_HEADER_SIZE
                    # Leer directorio con padding a dir_capacity, truncar a 2^global_depth
                    dir_bytes = f.read(self.dir_capacity * 4)
                    real_dir = 1 << self.global_depth
                    if len(dir_bytes) < real_dir * 4:
                        self._init_file()
                        return
                    unpacked = list(struct.unpack(f'{self.dir_capacity}i', dir_bytes))
                    self.directory = unpacked[:real_dir]
                    self._pages_base_offset_cached = None
                    return
                # Fallback: intentar header legacy (8 bytes)
                f.seek(off)
                header_old = f.read(FILE_HEADER_SIZE_OLD)
                if not header_old or header_old == b'\x00' * FILE_HEADER_SIZE_OLD:
                    self._init_file()
                    return
                self.read_count += 1
                gd, npi = struct.unpack(FILE_HEADER_FORMAT_OLD, header_old)
                self.global_depth, self.next_page_idx = gd, npi
                self.dir_capacity = max(2, (1 << self.global_depth))
                self._file_header_size = FILE_HEADER_SIZE_OLD
                dir_bytes = f.read((1 << self.global_depth) * 4)
                if len(dir_bytes) != (1 << self.global_depth) * 4:
                    self._init_file()
                    return
                self.directory = list(struct.unpack(f'{1 << self.global_depth}i', dir_bytes))
                self._pages_base_offset_cached = None
        except FileNotFoundError:
            self._init_file()

    def _init_file(self):
        self.global_depth = 1
        self.directory = [0, 1]
        self.next_page_idx = 2
        self.dir_capacity = 2
        self._file_header_size = FILE_HEADER_SIZE
        try:
            f = open(self.filename, 'r+b')
        except FileNotFoundError:
            f = open(self.filename, 'w+b')
        with f:
            off = self._json_offset()
            f.seek(off)
            # Escribir header nuevo y directorio con padding a capacidad
            f.write(struct.pack(FILE_HEADER_FORMAT, self.global_depth, self.next_page_idx, self.dir_capacity))
            padded_dir = self.directory + [-1] * (self.dir_capacity - len(self.directory))
            f.write(struct.pack(f'{self.dir_capacity}i', *padded_dir))
            self.write_count += 2
            # Invalidar cache
            self._pages_base_offset_cached = None

            f.seek(self._get_page_offset(0))
            f.write(struct.pack('i', 1))
            f.write(struct.pack('i', -1))
            f.write(b'\x00' * self.bucket_disk_size)

            self.write_count+=3

            f.seek(self._get_page_offset(1))
            f.write(struct.pack('i', 1))
            f.write(struct.pack('i', -1))
            f.write(b'\x00' * self.bucket_disk_size)

            self.write_count += 3

    def _read_bucket(self, page_idx):
        with open(self.filename, 'rb') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            local_depth = struct.unpack('i', f.read(4))[0]
            overflow_page = struct.unpack('i', f.read(4))[0]
            data = f.read(self.bucket_disk_size)
            self.read_count += 1
            return Bucket.unpack(data, local_depth, overflow_page,
                                 self.record_size, self.format, self.schema)

    def _write_bucket(self, page_idx, bucket):
        with open(self.filename, 'r+b') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            f.write(struct.pack('i', bucket.local_depth))
            f.write(struct.pack('i', bucket.overflow_page))
            f.write(bucket.pack(self.record_size, self.format, self.schema))
            self.write_count += 3

    def _write_directory(self):
        # Reescribe header y directorio con padding a capacidad
        with open(self.filename, 'r+b') as f:
            base = self._json_offset()
            f.seek(base)
            f.write(struct.pack(FILE_HEADER_FORMAT, self.global_depth, self.next_page_idx, self.dir_capacity))
            real_dir = 1 << self.global_depth
            padded_dir = self.directory + [-1] * (self.dir_capacity - real_dir)
            f.write(struct.pack(f'{self.dir_capacity}i', *padded_dir))
            self.write_count += 2
        self._pages_base_offset_cached = None

    def _grow_directory(self, new_capacity: int):
        """Aumenta la capacidad del directorio y reubica la región de páginas si cambia el offset."""
        new_capacity = max(new_capacity, 1 << self.global_depth)
        if new_capacity <= self.dir_capacity:
            return
        # Calcular offsets antiguos/nuevos y tamaño región páginas
        old_pages_off = self._pages_base_offset()
        page_span = HEADER_SIZE + self.bucket_disk_size
        region_size = self.next_page_idx * page_span
        new_pages_off = self._json_offset() + FILE_HEADER_SIZE + (new_capacity * 4)
        # Leer y mover región de páginas
        with open(self.filename, 'r+b') as f:
            f.seek(old_pages_off)
            data = f.read(region_size)
            f.seek(new_pages_off)
            f.write(data)
        self.dir_capacity = new_capacity
        self._write_directory()

    def _read_chain(self, page_idx):
        chain = []
        cur = page_idx
        while cur != -1:
            b = self._read_bucket(cur)
            chain.append((cur, b))
            cur = b.overflow_page
        return chain

    def find(self, key_value, key_name, unique=False):
        """
        Busca registros por key_value.
        Si unique=True, corta en cuanto encuentra el primer match.
        """
        if self.key_name is None:
            self.key_name = key_name
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        result = []
        
        cur = page_idx
        while cur != -1:
            bucket = self._read_bucket(cur)
            for rec in bucket.records:
                if rec.fields[key_name] == key_value and not rec.fields.get("deleted", False):
                    result.append({k: v for k, v in rec.fields.items() if k != 'deleted'})
                    if unique:
                        return result 
            cur = bucket.overflow_page
        
        return result

    def _chain_bit_counts(self, chain, bit_pos):
        zeros = ones = 0
        key = self.key_name or "id"
        for _, b in chain:
            for rec in b.records:
                try:
                    h = self._hash(rec.fields[key])
                    if (h >> bit_pos) & 1:
                        ones += 1
                    else:
                        zeros += 1
                except Exception:
                    pass
        return zeros, ones

    def insert(self, record_data, key_name):
        if self.key_name is None:
            self.key_name = key_name

        key_value = record_data[key_name]
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        chain = self._read_chain(page_idx)

        # 1) intentar insertar en algún bucket de la cadena
        for curr_page, curr_bucket in chain:
            if not curr_bucket.is_full():
                rec = Record(self.schema, self.format, record_data)
                curr_bucket.put(rec)
                self._write_bucket(curr_page, curr_bucket)
                return record_data

        # 2) si la cadena aún no alcanzó el máximo -> encadenar otro bucket (chaining)
        if len(chain) < self._max_chain_length():
            last_page, last_bucket = chain[-1]
            new_overflow_page = self.next_page_idx
            self.next_page_idx += 1

            last_bucket.overflow_page = new_overflow_page
            self._write_bucket(last_page, last_bucket)

            new_overflow = Bucket(local_depth=last_bucket.local_depth, overflow_page=-1)
            rec = Record(self.schema, self.format, record_data)
            new_overflow.put(rec)
            self._write_bucket(new_overflow_page, new_overflow)

            self._write_directory()
            return record_data

        # 3) si se alcanzó el máximo de la cadena -> intentar split
        head_bucket = chain[0][1]
        old_local = head_bucket.local_depth
        zeros, ones = self._chain_bit_counts(chain, old_local)
        splittable = (zeros > 0 and ones > 0)

        self._split(dir_idx, page_idx, chain)

        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        new_chain = self._read_chain(page_idx)
        for curr_page, curr_bucket in new_chain:
            if not curr_bucket.is_full():
                rec = Record(self.schema, self.format, record_data)
                curr_bucket.put(rec)
                self._write_bucket(curr_page, curr_bucket)
                return record_data

        # 4) fallback: si el split no ayudó
        last_page, last_bucket = new_chain[-1]
        new_overflow_page = self.next_page_idx
        self.next_page_idx += 1

        last_bucket.overflow_page = new_overflow_page
        self._write_bucket(last_page, last_bucket)

        new_overflow = Bucket(local_depth=last_bucket.local_depth, overflow_page=-1)
        rec = Record(self.schema, self.format, record_data)
        new_overflow.put(rec)
        self._write_bucket(new_overflow_page, new_overflow)

        self._write_directory()
        return record_data

    def _split(self, dir_idx, page_idx, chain):
        head_bucket = chain[0][1]
        old_local = head_bucket.local_depth

        if old_local == self.global_depth:
            self.directory = self.directory + self.directory
            self.global_depth += 1
            # Asegurar capacidad suficiente (doblar si es necesario)
            if (1 << self.global_depth) > self.dir_capacity:
                self._grow_directory(max(self.dir_capacity * 2, 1 << self.global_depth))

        new_page_idx = self.next_page_idx
        self.next_page_idx += 1

        old_records = []
        for _, b in chain:
            old_records.extend(b.records)

        for curr_page, curr_bucket in chain:
            curr_bucket.records = []
            curr_bucket.overflow_page = -1
            self._write_bucket(curr_page, curr_bucket)

        head_bucket.local_depth = old_local + 1
        head_bucket.records = []
        head_bucket.overflow_page = -1

        new_bucket = Bucket(local_depth=old_local + 1, overflow_page=-1)

        stride_bit = 1 << old_local
        for i in range(len(self.directory)):
            if self.directory[i] == page_idx:
                if (i & stride_bit) != 0:
                    self.directory[i] = new_page_idx
                else:
                    self.directory[i] = page_idx

        self._write_bucket(page_idx, head_bucket)
        self._write_bucket(new_page_idx, new_bucket)
        self._write_directory()

        for rec in old_records:
            self.insert(rec.fields, self.key_name)

    def remove(self, key_value, key_name="id", unique=False):
        """
        Remueve registros que coincidan con key_value.
        Si unique=True, corta en cuanto remueva el primero.
        """
        if self.key_name is None:
            self.key_name = key_name
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        removed = []
        
        # Recorrer cadena bucket por bucket
        cur = page_idx
        while cur != -1:
            bucket = self._read_bucket(cur)
            rem = bucket.remove(key_value, key_name)
            if rem:
                self._write_bucket(cur, bucket)
                removed.extend([r.fields for r in rem])
                if unique:
                    return removed  # Cortar si unique y ya removimos uno
            cur = bucket.overflow_page
        
        return removed if removed else None

    def get_all_records(self):
        all_records = []
        seen_pages = set()
        for page_idx in self.directory:
            if page_idx in seen_pages:
                continue
            chain = self._read_chain(page_idx)
            for curr_page, curr_bucket in chain:
                if curr_page in seen_pages:
                    continue
                seen_pages.add(curr_page)
                for record in curr_bucket.records:
                    if not record.fields.get("deleted", False):
                        all_records.append(record.fields)
        return all_records