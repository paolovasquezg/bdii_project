# Testing.py
# Prueba autónoma del R-Tree con RIDs sintéticos (sin depender del Heap).
# - Crea la tabla para tener la ruta base
# - Inserta puntos en el R-Tree externo
# - Ejecuta range_search y knn pasando el mainfile como "heap" (aunque no lo usemos)

import os
from Methods import get_filename
from TableCreate import create_table
from indexes.rtree.RTree import RTreeFile

TABLE = "Restaurantes"

def ensure_table_only_for_paths():
    """
    Creamos (si no existe) una tabla mínima sólo para tener la ruta base en files/{TABLE}/.
    No insertamos al heap para evitar conflictos con Record/Heap.
    """
    filename = get_filename(TABLE)
    if filename is None:
        fields = [
            {"name": "id", "type": "int", "key": "primary", "index": "heap"},
            {"name": "nombre", "type": "varchar", "length": 20},
            # 'ubicacion' no se guarda en el heap en esta prueba
        ]
        create_table(TABLE, fields)
        filename = get_filename(TABLE)
        print(f"[OK] Tabla {TABLE} creada en {filename}.")
    else:
        print(f"[INFO] Tabla {TABLE} ya existe en {filename}.")
    return filename

def rtree_filename(mainfile: str) -> str:
    base_dir = os.path.dirname(mainfile)
    idx_dir = os.path.join(base_dir, "idx", "ubicacion")
    os.makedirs(idx_dir, exist_ok=True)
    return os.path.join(idx_dir, "rtree.idx")

def seed_points(rtpath: str):
    """
    Inserta puntos en el R-Tree usando RID sintético (pos=i, slot=0).
    """
    points = [
        (-12.10,  -77.00),
        (-12.090, -77.010),
        (-12.110, -77.020),
        (-12.095, -77.005),
        (-12.101, -77.008),
        (-12.085, -77.030),
    ]
    rtree = RTreeFile(rtpath)
    rtree.open()
    for i, (x, y) in enumerate(points):
        rec = {"pos": i, "slot": 0, "deleted": False, "ubicacion": [x, y]}
        rtree.insert(rec, {"key": "ubicacion"})
    rtree.close()
    print(f"[OK] Insertados {len(points)} puntos en {rtpath}")

def run_range_search(mainfile: str, rtpath: str):
    """
    Búsqueda por rango: punto + radio
    """
    point = [-12.10, -77.00]
    r = 0.012
    rtree = RTreeFile(rtpath)
    rtree.open()
    # >>> PASAMOS mainfile como 'heap' para que RTree.py no falle al instanciar HeapFile
    out = rtree.range_search({"point": point, "radio": r, "heap": mainfile})
    rtree.close()
    print("[RANGE_SEARCH] resultados (RID sintético y MBR):")
    for rec in out:
        print(rec)

def run_knn(mainfile: str, rtpath: str):
    """
    K vecinos más cercanos
    """
    point = [-12.10, -77.00]
    k = 3
    rtree = RTreeFile(rtpath)
    rtree.open()
    # >>> PASAMOS mainfile como 'heap'
    out = rtree.knn({"point": point, "k": k, "heap": mainfile})
    rtree.close()
    print("[KNN] resultados (RID sintético y MBR):")
    for rec in out:
        print(rec)

if __name__ == "__main__":
    mainfile = ensure_table_only_for_paths()
    rtpath = rtree_filename(mainfile)
    seed_points(rtpath)
    run_range_search(mainfile, rtpath)
    run_knn(mainfile, rtpath)
