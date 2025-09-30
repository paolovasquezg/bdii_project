# indexes/rtree/storage.py
import os, struct, io
from typing import List
from .node import Node, Entry

# Header binario del archivo R-Tree
HEADER_FMT = "<4sBHHIHIQQ"  # magic,ver,M,m,root,height,page_size,pages_read,pages_written
MAGIC = b"RTRE"
VERSION = 1
DEFAULT_PAGE_SIZE = 4096

class Storage:
    def __init__(self, filename: str, M: int = 32):
        self.filename = filename
        self.M = M
        self.m = max(2, M // 2)
        self.page_size = DEFAULT_PAGE_SIZE
        self.root = 0
        self.height = 1
        self.pages_read = 0
        self.pages_written = 0

    def open(self):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as f:
                f.write(struct.pack(HEADER_FMT, MAGIC, VERSION, self.M, self.m,
                                    0, self.height, self.page_size,
                                    self.pages_read, self.pages_written))
                f.write(b"\x00" * self.page_size)  # página 0
        else:
            with open(self.filename, "rb") as f:
                hdr = f.read(struct.calcsize(HEADER_FMT))
                try:
                    magic, ver, M, m, root, height, page_size, r, w = struct.unpack(HEADER_FMT, hdr)
                except struct.error:
                    magic, ver = b"????", -1  # archivo sucio
            # si el archivo estaba sucio (por ej., JSON), recrear
            if magic != MAGIC or ver != VERSION:
                with open(self.filename, "wb") as f:
                    self.root = 0; self.height = 1
                    self.pages_read = 0; self.pages_written = 0
                    f.write(struct.pack(HEADER_FMT, MAGIC, VERSION, self.M, self.m,
                                        0, self.height, self.page_size,
                                        self.pages_read, self.pages_written))
                    f.write(b"\x00" * self.page_size)
            else:
                self.M, self.m, self.root, self.height = M, m, root, height
                self.page_size = page_size
                self.pages_read, self.pages_written = r, w

    def close(self):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack(HEADER_FMT, MAGIC, VERSION, self.M, self.m,
                                self.root, self.height, self.page_size,
                                self.pages_read, self.pages_written))

    # ---- páginas
    def alloc_page(self) -> int:
        size = os.path.getsize(self.filename)
        header = struct.calcsize(HEADER_FMT)
        used_pages = (size - header) // self.page_size
        with open(self.filename, "ab") as f:
            f.write(b"\x00" * self.page_size)
        return used_pages

    # ---- nodos
    def write_node(self, node: Node):
        buf = io.BytesIO()
        buf.write(struct.pack("<B H", 1 if node.is_leaf else 0, len(node.entries)))
        for e in node.entries:
            buf.write(struct.pack("<ffff", *e.mbr))
            if node.is_leaf:
                page, slot = e.rid
                buf.write(struct.pack("<IH", page, slot))
            else:
                buf.write(struct.pack("<I", e.child))
        data = buf.getvalue()
        if len(data) > self.page_size:
            raise ValueError("Node overflow page_size (reduce M).")
        data += b"\x00" * (self.page_size - len(data))
        with open(self.filename, "r+b") as f:
            f.seek(struct.calcsize(HEADER_FMT) + node.page_id * self.page_size)
            f.write(data)
        self.pages_written += 1

    def read_node(self, page_id: int) -> Node:
        with open(self.filename, "rb") as f:
            f.seek(struct.calcsize(HEADER_FMT) + page_id * self.page_size)
            raw = f.read(self.page_size)
        self.pages_read += 1
        is_leaf, count = struct.unpack_from("<B H", raw, 0)
        off = 3
        entries: List[Entry] = []
        for _ in range(count):
            xmin, xmax, ymin, ymax = struct.unpack_from("<ffff", raw, off); off += 16
            if is_leaf:
                page, slot = struct.unpack_from("<IH", raw, off); off += 6
                entries.append(Entry(mbr=(xmin, xmax, ymin, ymax), rid=(page, slot)))
            else:
                child, = struct.unpack_from("<I", raw, off); off += 4
                entries.append(Entry(mbr=(xmin, xmax, ymin, ymax), child=child))
        return Node(page_id=page_id, is_leaf=bool(is_leaf), entries=entries, M=self.M)
