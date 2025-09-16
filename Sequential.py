from Methods import get_json, build_format
from Record import Record
import struct

class HeapFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0] 
        self.format = build_format(self.schema)
        self.REC_SIZE = struct.calcsize(self.format)