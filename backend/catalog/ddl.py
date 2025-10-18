from pathlib import Path
from typing import List, Dict, Optional

from backend.catalog.settings import DATA_DIR
from backend.catalog.catalog import load_tables, save_tables, put_json, table_meta_path, get_json
import shutil


from backend.storage.indexes.hash import ExtendibleHashingFile
from backend.storage.indexes.heap import HeapFile
from backend.storage.indexes.sequential import SeqFile
from backend.storage.indexes.isam import IsamFile
from backend.storage.indexes.bplus import BPlusFile
from backend.storage.file import File


def get_physical_records(mainfilename: str, main_index: str, pos: bool):

    records = []
    
    if main_index == "heap":
        GetFile = HeapFile(mainfilename)
        records = GetFile.get_all(pos)
    elif main_index == "sequential":
        GetFile = SeqFile(mainfilename)
        records = GetFile.get_all()
    elif main_index == "isam":
        GetFile = IsamFile(mainfilename)
        records = GetFile.get_all()
    else:
        GetFile = BPlusFile(mainfilename)
        records = GetFile.get_all()
    
    return records

def backfill_secondary(table: str, column: str, relation: dict, indexes: dict):
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

        records = get_physical_records(main, prim_kind, True)

        for record in records:
            if prim_kind == "heap":
                rec = {"pos": record[1], column: record[0][column], "deleted": False}
            else:
                rec = {"pk": record[pk_name], column: record[column], "deleted": False}
            try:
                h.insert(rec, column)
            except Exception:
                pass

    elif sec_kind == "bplus":
        bp = BPlusFile(sec_file)
        records = get_physical_records(main, prim_kind, True)
        for record in records:
            if prim_kind == "heap":
                rec = {"pos": record[1], column: record[0][column], "deleted": False}
            else:
                rec = {"pk": record[pk_name], column: record[column], "deleted": False}
            try:
                bp.insert(rec, {"key": column})
            except Exception:
                pass

    elif sec_kind == "rtree":
        records = get_physical_records(main, prim_kind, True)

        try:
            from pathlib import Path as _P
            import os as _os
            data_dir = _os.path.dirname(_os.path.dirname(sec_file))
            idx_dir = _P(data_dir) / table
            idx_path = idx_dir / f"{table}_rtree_{column}.idx"
            map_path = idx_dir / f"{table}_rtree_{column}.map.json"
            idx_path.unlink(missing_ok=True)
            map_path.unlink(missing_ok=True)
        except Exception:
            pass

        file_inst = File(table)
        rt = file_inst._make_rtree(column, heap_ok=(prim_kind=="heap"), reuse_cached=True)
        if prim_kind == "heap":
            for row_dict, pos in records:
                if column not in row_dict: continue
                ok, pt = file_inst._as_point(row_dict[column])
                if not ok: continue
                in_rec = {"pos": pos, column: pt, "deleted": False}
                rt.insert(in_rec)
        else:
            for row_dict in records:
                if column not in row_dict: continue
                ok, pt = file_inst._as_point(row_dict[column])
                if not ok: continue
                in_rec = {"pk": row_dict[pk_name], column: pt, "deleted": False}
                rt.insert(in_rec)
        # Close cached rtrees to persist headers
        file_inst._close_cached_rtrees()

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

    prim_schema = list(new_schema)
    prim_schema.append({"name": "deleted", "type": "?"})
    put_json(mainfilename, [prim_schema])

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

def get_table_descp(relation: dict, indexes: dict):

    fields = []

    for key in relation:

        field = relation[key]

        field["name"] = key

        fields.append(field)
    
    for index in indexes:

        if index != "primary":

            for i in range(len(fields)):

                if index == fields[i]["name"]:
                    fields[i]["index"] = indexes[index]["index"]

    return fields

def add_index(fields: list[dict], column: str, method: str):

    for i in range(len(fields)):

        if fields[i]["name"] == column:
            fields[i]["index"] = method
            break

    return fields

def delete_index(fields: list[dict], column: str):
    for i in range(len(fields)):

        if fields[i]["name"] == column:
            del fields[i]["index"]
            break

    return fields


def create_index(table: str, column: str, method: str):
    """
    Crea un índice secundario en DATA_DIR/<table>/<table>-<methodToken>-<column>.dat
    y actualiza el metadato <table>.dat (indexes[column]).
    No recrea si ya existe.
    """
    meta = table_meta_path(table)
    relation, indexes = get_json(str(meta), 2)

    if not table or column in indexes:
        return
    
    if "key" in relation[column] and relation[column]["key"] == "primary":
        if method == "hash" or method == "rtree":
            return

        mainfilename = indexes["primary"]["filename"]
        main_index = indexes["primary"]["index"]

        records = get_physical_records(mainfilename, main_index, True)

        if main_index == "heap":
            records = [row for (row, _pos) in records]

        table_desp = get_table_descp(relation, indexes)
        table_desp = add_index(table_desp, column, method)

        drop_table(table)
        create_table(table, table_desp)

        InsFile = File(table)
        InsFile.execute({"op": "build", "records": records})

        try:
            InsFile._close_cached_rtrees()
        except Exception:
            pass
    
    else:
        kind = _canon_index_kind(method)
        token = _filename_token(method)

        idx_path = DATA_DIR / table / f"{table}_{token}_{column}.dat"
        idx_file = str(idx_path)

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

        put_json(idx_file, [idx_schema])
        indexes[column] = {"index": kind, "filename": idx_file}
        put_json(str(meta), [relation, indexes])
        try:
            backfill_secondary(table, column, relation, indexes)
        except Exception as e:
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
    
    if "key" in relation[col] and relation[col]["key"] == "primary":
        mainfilename = indexes["primary"]["filename"]
        main_index = indexes["primary"]["index"]

        records = get_physical_records(mainfilename, main_index, True)
  
        if main_index == "heap":
            records = [row for (row, _pos) in records]

        table_desp = get_table_descp(relation, indexes)
        table_desp = delete_index(table_desp, col)

        drop_table(table)
        create_table(table, table_desp)

        InsFile = File(table)
        InsFile.execute({"op": "build", "records": records})
        try:
            InsFile._close_cached_rtrees()
        except Exception:
            pass

    else:
        try:
            Path(indexes[col]["filename"]).unlink(missing_ok=True)
        except Exception:
            pass

        del indexes[col]
        put_json(str(meta), [relation, indexes])