import struct

class Record:
    def __init__(self, schema: list, format: str, values):
        self.schema = schema
        self.fields = {field["name"]: None for field in schema}
        self.fields.update(values)
        self.format = format

    def pack(self):
        values = []
        for field in self.schema:
            name = field["name"]
            ftype = field["type"]
            length = field.get("length", 1)
            val = self.fields[name]
            t = ftype.lower()
            if t in ("i", "int", "integer"):
                val = int(val or 0)
            elif t in ("h", "smallint"):
                val = int(val or 0)
            elif t in ("q", "bigint"):
                val = int(val or 0)
            elif t in ("f", "float", "real"):
                val = float(val or 0.0)
            elif t in ("d", "double", "double precision"):
                val = float(val or 0.0)
            elif t in ("c", "char"):
                val = (val or b"" if isinstance(val, bytes) else str(val or "").encode("utf-8")).ljust(length, b"\x00")
            elif t in ("s", "varchar", "string"):
                val = (val or b"" if isinstance(val, bytes) else str(val or "").encode("utf-8")).ljust(length, b"\x00")
            elif t in ("b", "bool", "boolean", "?"):
                val = bool(val)
            elif t in ("blob", "binary"):
                val = (val or b"").ljust(length, b"\x00")
            elif t in ("date", "datetime"):
                val = (val or b"" if isinstance(val, bytes) else str(val or "").encode("utf-8")).ljust(length, b"\x00")
            values.append(val)
        return struct.pack(self.format, *values)

    @classmethod
    def unpack(cls, data, format, schema):
        unpacked = struct.unpack(format, data)
        values = {}
        for field, raw in zip(schema, unpacked):
            name = field["name"]
            ftype = field["type"]
            t = ftype.lower()
            if t in ("i", "int", "integer"):
                values[name] = int(raw)
            elif t in ("h", "smallint"):
                values[name] = int(raw)
            elif t in ("q", "bigint"):
                values[name] = int(raw)
            elif t in ("f", "float", "real"):
                values[name] = float(raw)
            elif t in ("d", "double", "double precision"):
                values[name] = float(raw)
            elif t in ("c", "char"):
                values[name] = raw.decode("utf-8").rstrip("\x00 ")
            elif t in ("s", "varchar", "string"):
                values[name] = raw.decode("utf-8").rstrip("\x00 ")
            elif t in ("b", "bool", "boolean", "?"):
                values[name] = bool(raw)
            elif t in ("blob", "binary"):
                values[name] = raw
            elif t in ("date", "datetime"):
                values[name] = raw.decode("utf-8").rstrip("\x00 ")
            else:
                values[name] = raw
        return cls(schema, format, values)
    
    def __str__(self):
        return f"Record({{ {', '.join(f'{field["name"]}: {repr(self.fields[field["name"]])}' for field in self.schema)} }})"
    
