from pathlib import Path
from typing import List, Dict, Optional

from .settings import DATA_DIR
from backend.catalog.catalog import load_tables, save_tables, put_json, table_meta_path

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

        # índices declarados
        if "index" in field:
            method = field["index"]
            # sequential/isam solo si es primario
            if method in ("sequential", "isam") and field.get("key") != "primary":
                return  # o raise ValueError

            # rutas dentro de DATA_DIR (¡no "files/..." suelto!)
            idx_path = DATA_DIR / table / f"{table}-{method}-{name}.dat"
            index = {"index": method, "filename": str(idx_path)}
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
        idx_path = DATA_DIR / table / f"{table}-heap-{pk_name}.dat"
        indexes["primary"] = {"index": "heap", "filename": str(idx_path)}

    # 5) metadato principal bajo DATA_DIR
    table_file = table_meta_path(table)      # e.g. runtime/files/<table>/<table>.dat
    put_json(str(table_file), [new_fields, indexes])

    # 6) archivo del primario (copia del schema + deleted)
    mainfilename = indexes["primary"]["filename"]
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

def create_index(table: str, column: str, method: str):
    """
    Crea un índice secundario en DATA_DIR/<table>/<table>-<method>-<column>.dat
    y actualiza el metadato <table>.dat (indexes[column]).
    No recrea si ya existe.
    """
    meta = table_meta_path(table)  # p.ej. runtime/files/products/products.dat
    relation, indexes = get_json(str(meta), 2)

    # ya existe
    if column in indexes:
        return

    # ruta del archivo índice
    idx_path = DATA_DIR / table / f"{table}-{method}-{column}.dat"
    idx_file = str(idx_path)

    # schema: [col, pk, deleted]
    col_spec = {"name": column, **relation.get(column, {})}
    pk_name = _find_pk_name(relation)
    if not pk_name:
        raise ValueError(f"No se pudo determinar PK para la tabla {table}")
    pk_spec = relation[pk_name]

    idx_schema = [col_spec]
    if "length" in pk_spec:
        idx_schema.append({"name": "pk", "type": pk_spec["type"], "length": pk_spec["length"]})
    else:
        idx_schema.append({"name": "pk", "type": pk_spec["type"]})
    idx_schema.append({"name": "deleted", "type": "?"})

    # persistir índice secundario vacío y actualizar metadato
    put_json(idx_file, [idx_schema])
    indexes[column] = {"index": method, "filename": idx_file}
    put_json(str(meta), [relation, indexes])

def drop_index(table: Optional[str], column_or_name: Optional[str]):
    """
    Elimina un índice secundario del metadato y borra su archivo.
    Ignora si el índice no existe. No elimina 'primary'.
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
