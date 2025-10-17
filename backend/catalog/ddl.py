from pathlib import Path
from typing import List, Dict, Optional

from .settings import DATA_DIR
from backend.catalog.catalog import load_tables, save_tables, put_json, table_meta_path

import struct
from backend.core.utils import build_format
from backend.core.record import Record
from backend.storage.indexes.hash import ExtendibleHashingFile
from backend.storage.indexes.bplus import BPlusFile
from backend.storage.file import File


def _iter_primary_rows_with_pos(mainfilename: str):
    """
    Lee el archivo físico principal y devuelve (row_dict, pos) de cada registro no borrado.
    Sirve para cualquier primario (heap, sequential, isam, bplus) porque el layout
    binario es el mismo: [4 bytes tam_schema][schema JSON][registros fijos].
    """
    schema = get_json(mainfilename)[0]              # 1) esquema
    fmt = build_format(schema)
    rec_size = struct.calcsize(fmt)

    with open(mainfilename, "rb") as f:
        schema_size = struct.unpack("<I", f.read(4))[0]
        f.seek(0, 2); end = f.tell()
        f.seek(4 + schema_size)

        while f.tell() < end:
            pos = f.tell()
            blob = f.read(rec_size)
            if not blob or len(blob) < rec_size:
                break
            rec = Record.unpack(blob, fmt, schema)  # -> objeto con .fields
            row = getattr(rec, "fields", {}) or {}
            if not row.get("deleted", False):
                yield row, pos

def _backfill_secondary(table: str, column: str, relation: dict, indexes: dict):
    """
    Llena el índice secundario de 'column' con los datos existentes del primario.
    """
    pk_name = _find_pk_name(relation)
    if pk_name is None:
        return

    main = indexes["primary"]["filename"]
    prim_kind = indexes["primary"]["index"]
    sec_kind  = indexes[column]["index"]
    sec_file  = indexes[column]["filename"]

    if sec_kind == "hash":
        h = ExtendibleHashingFile(sec_file)
        for row, pos in _iter_primary_rows_with_pos(main):
            if column not in row:
                continue
            if prim_kind == "heap":
                rec = {"pos": pos, column: row[column], "deleted": False}
            else:
                rec = {"pk": row[pk_name], column: row[column], "deleted": False}
            try:
                h.insert(rec, key_name=column)
            except Exception:
                pass

    elif sec_kind == "bplus":
        bp = BPlusFile(sec_file)
        for row, pos in _iter_primary_rows_with_pos(main):
            if column not in row:
                continue
            if prim_kind == "heap":
                rec = {"pos": pos, column: row[column], "deleted": False}
            else:
                rec = {column: row[column], "pk": row[pk_name], "deleted": False}
            try:
                bp.insert(rec, {"key": column})
            except Exception:
                pass

def _canon_index_kind(method: str) -> str:
    m = (method or "").strip().lower().replace(" ", "")
    if m in ("b+", "bplus", "btree", "b-tree"):
        return "bplus"
    if m in ("r-tree", "rtree", "r+tree", "rplus"):
        return "rtree"
    if m in ("seq", "sequential"):
        return "sequential"
    if m in ("isam",):
        return "isam"
    if m in ("hash",):
        return "hash"
    if m in ("heap",):
        return "heap"
    # fallback sin romper
    return m or "heap"

def _filename_token(method: str) -> str:
    k = _canon_index_kind(method)
    if k == "bplus":
        return "bplus"
    # las demás usan su nombre canónico
    return k


def create_table(table: str, fields: List[Dict]):
    # 1) carpeta bajo DATA_DIR
    (DATA_DIR / table).mkdir(parents=True, exist_ok=True)

    # 2) evitar recrear
    tables = load_tables()
    if table in tables:
        return

    indexes: Dict[str, Dict] = {}
    new_fields: Dict[str, Dict] = {}
    new_schema: List[Dict] = []
    pk_name: Optional[str] = None

    for field in fields:
        name = field["name"]

        # schema lógico
        new_fields[name] = {"type": field["type"]}
        if "key" in field:
            new_fields[name]["key"] = field["key"]
        if "length" in field:
            new_fields[name]["length"] = field["length"]

        if field.get("key") == "primary":
            pk_name = name

        # índices declarados inline (PRIMARY KEY USING ..., INDEX USING ...)
        if "index" in field:
            method_raw = field["index"]
            kind = _canon_index_kind(method_raw)
            token = _filename_token(method_raw)

            # sequential/isam solo si es primario
            if kind in ("sequential", "isam") and field.get("key") != "primary":
                return  # o raise ValueError

            # rutas dentro de DATA_DIR (¡no "files/..." suelto!)
            idx_path = DATA_DIR / table / f"{table}_{token}_{name}.dat"
            index = {"index": kind, "filename": str(idx_path)}
            if field.get("key") == "primary":
                indexes["primary"] = index
            indexes[name] = index

        # schema físico para el primario (sin metacampos)
        nf = {k: v for k, v in field.items() if k not in ("index", "key")}
        new_schema.append(nf)

    # 3) PK por defecto si no se declaró
    if pk_name is None and fields:
        pk_name = fields[0]["name"]

    # 4) índice primario por defecto (heap sobre la PK)
    if "primary" not in indexes:
        token = _filename_token("heap")
        idx_path = DATA_DIR / table / f"{table}_{token}_{pk_name}.dat"
        indexes["primary"] = {"index": "heap", "filename": str(idx_path)}

    # 5) metadato principal bajo DATA_DIR
    table_file = table_meta_path(table)      # e.g. runtime/files/<table>/<table>.dat
    put_json(str(table_file), [new_fields, indexes])

    # 6) archivo del primario (copia del schema + deleted)
    mainfilename = indexes["primary"]["filename"]
    prim_kind = indexes["primary"]["index"]

    if prim_kind in ("heap", "sequential", "isam"):
        prim_schema = list(new_schema)
        prim_schema.append({"name": "deleted", "type": "?"})
        put_json(mainfilename, [prim_schema])
    else:
        # bplus / rtree / hash como primario → que la implementación lo inicialice
        Path(mainfilename).write_bytes(b"")

    # 7) archivos de índices secundarios (si hay)
    for col, info in indexes.items():
        if col == "primary":
            continue
        if info["filename"] == mainfilename:
            continue

        for sch in new_schema:
            if sch["name"] == col:
                idx_schema = [sch]
                primary_index_type = indexes.get("primary", {}).get("index", "heap")
                if primary_index_type == "heap":
                    idx_schema.append({"name": "pos", "type": "i"})
                else:
                    pk_spec = new_fields[pk_name]
                    if "length" in pk_spec:
                        idx_schema.append({"name": "pk", "type": pk_spec["type"], "length": pk_spec["length"]})
                    else:
                        idx_schema.append({"name": "pk", "type": pk_spec["type"]})

                idx_schema.append({"name": "deleted", "type": "?"})
                put_json(info["filename"], [idx_schema])
                break

    # 8) registrar la tabla
    tables[table] = str(table_file)
    save_tables(tables)

# --- NUEVO ---
import shutil

# importa get_json también (ajusta import arriba si falta)
try:
    from .catalog import get_json
except ImportError:
    from backend.catalog import get_json

def _find_pk_name(relation: Dict[str, Dict]) -> Optional[str]:
    for col, spec in relation.items():
        if spec.get("key") == "primary":
            return col
    # fallback: primera columna si no hay key registrada
    return next(iter(relation.keys()), None)

def drop_table(table: str):
    """
    Elimina la carpeta DATA_DIR/<table>, borra archivos y saca la entrada de tables.dat.
    """
    # borra carpeta física
    tdir = DATA_DIR / table
    if tdir.exists():
        shutil.rmtree(tdir, ignore_errors=True)

    # quitar del catálogo global
    tables = load_tables()
    if table in tables:
        del tables[table]
        save_tables(tables)

def create_index(table: str, column: str, method: str):
    """
    Crea un índice secundario en DATA_DIR/<table>/<table>-<methodToken>-<column>.dat
    y actualiza el metadato <table>.dat (indexes[column]).
    No recrea si ya existe.
    """
    meta = table_meta_path(table)  # p.ej. runtime/files/products/products.dat
    relation, indexes = get_json(str(meta), 2)

    # ya existe
    if column in indexes:
        return

    # normalizar metodo
    kind = _canon_index_kind(method)
    token = _filename_token(method)

    # ruta del archivo índice
    idx_path = DATA_DIR / table / f"{table}_{token}_{column}.dat"
    idx_file = str(idx_path)

    # schema: [col, pk/pos, deleted]
    col_spec = {"name": column, **relation.get(column, {})}
    pk_name = _find_pk_name(relation)
    if not pk_name:
        raise ValueError(f"No se pudo determinar PK para la tabla {table}")
    pk_spec = relation[pk_name]

    idx_schema = [col_spec]
    primary_index_type = indexes.get("primary", {}).get("index", "heap")
    if primary_index_type == "heap":
        idx_schema.append({"name": "pos", "type": "i"})
    else:
        if "length" in pk_spec:
            idx_schema.append({"name": "pk", "type": pk_spec["type"], "length": pk_spec["length"]})
        else:
            idx_schema.append({"name": "pk", "type": pk_spec["type"]})
    idx_schema.append({"name": "deleted", "type": "?"})

    # persistir índice secundario vacío y actualizar metadato
    put_json(idx_file, [idx_schema])
    indexes[column] = {"index": kind, "filename": idx_file}
    put_json(str(meta), [relation, indexes])
    try:
        _backfill_secondary(table, column, relation, indexes)
    except Exception as e:
        # Si algo sale mal, al menos no dejamos roto el DDL
        # (podés loggear 'e' si querés)
        pass

def drop_index(table: Optional[str], column_or_name: Optional[str]):
    """
    Elimina un índice secundario del metadato y borra su archivo.
    Ignora si el índice no existe. No elimina 'indexes'.
    """
    if not table or not column_or_name:
        return
    meta = table_meta_path(table)
    relation, indexes = get_json(str(meta), 2)

    col = column_or_name
    if col == "primary" or col not in indexes:
        # no eliminamos índice primario desde aquí
        return

    # borrar archivo del índice (si existe)
    try:
        Path(indexes[col]["filename"]).unlink(missing_ok=True)
    except Exception:
        pass

    # quitar del metadato
    del indexes[col]
    put_json(str(meta), [relation, indexes])