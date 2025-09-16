import pickle
import os
import json
import struct

def load_tables(Tables_File: str = "files/tables.dat"):
    if os.path.exists(Tables_File):
        with open(Tables_File, "rb") as f:
            return pickle.load(f)
    return {}

def save_tables(catalog: dict, Tables_File: str = "files/tables.dat"):
    with open(Tables_File, "wb") as f:
        pickle.dump(catalog, f)

def get_filename(table: str):
    tables = load_tables()

    if table not in tables:
        return None

    return tables[table]

def get_json(filename: str, n: int = 1):
    result = []
    with open(filename, "rb") as f:
        for _ in range(n):
            size_bytes = f.read(4)
            if not size_bytes or len(size_bytes) < 4:
                break
            size = struct.unpack("I", size_bytes)[0]
            data = f.read(size)
            if not data or len(data) < size:
                break
            result.append(json.loads(data.decode("utf-8")))
    return result

def put_json(filename: str, data):
    if not isinstance(data, list):
        data = [data]
    with open(filename, "wb") as f:
        for item in data:
            out = json.dumps(item).encode("utf-8")
            f.write(struct.pack("I", len(out)))
            f.write(out)

def build_format(schema):
    fmt = ""
    for field in schema:
        ftype = field["type"]
        length = field.get("length", 1)
        t = ftype.lower()
        
        if t in ("i", "int", "integer"):
            fmt += "i"
        elif t in ("h", "smallint"):
            fmt += "h"
        elif t in ("q", "bigint"):
            fmt += "q"
        elif t in ("f", "float", "real"):
            fmt += "f"
        elif t in ("d", "double", "double precision"):
            fmt += "d"
        elif t in ("c", "char"):
            fmt += f"{length}s"
        elif t in ("s", "varchar", "string"):
            fmt += f"{length}s"
        elif t in ("b", "bool", "boolean", "?"):
            fmt += "?"
        elif t in ("blob", "binary"):
            fmt += f"{length}s"
        elif t in ("date", "datetime"):
            fmt += f"{length}s"
    return fmt