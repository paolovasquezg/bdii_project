import os
import pickle
from pathlib import Path
import struct
import json
from .settings import DATA_DIR

def table_dir(name: str) -> Path: return DATA_DIR / name
def table_meta_path(name: str) -> Path: return table_dir(name) / f"{name}.dat"

TABLES_FILE = DATA_DIR / "tables.dat"

import json, os, struct

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

def get_filename(table: str) -> str:
    return str(table_meta_path(table))

def load_tables() -> dict:
    if TABLES_FILE.exists():
        with TABLES_FILE.open("rb") as f:
            return pickle.load(f)
    return {}

def save_tables(catalog: dict) -> None:
    TABLES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TABLES_FILE.open("wb") as f:
        pickle.dump(catalog, f)