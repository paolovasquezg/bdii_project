import os, shutil
from methods.TableCreate import create_table
from indexes.File import File

TAB = "Restaurantes"
BASE = "files"   

def clean():
    shutil.rmtree(BASE, ignore_errors=True)

def ensure_table():
    create_table(TAB, [
        
        {"name":"id", "type":"i", "key":"primary", "index":"heap"},
        {"name":"nombre", "type":"s", "length":20},
        
        {"name":"ubicacion", "type":"a", "length":2, "type_items":"f", "index":"rtree"}
    ])

def run_inserts(db: File):
    db.execute({"op":"insert","record":{"id":1,"nombre":"Tanta",   "ubicacion":[-12.10,-77.00]}})
    db.execute({"op":"insert","record":{"id":2,"nombre":"Pardos",  "ubicacion":[-12.09,-77.01]}})
    db.execute({"op":"insert","record":{"id":3,"nombre":"Norkys",  "ubicacion":[-12.11,-77.02]}})
    db.execute({"op":"insert","record":{"id":4,"nombre":"LaLucha", "ubicacion":[-12.105,-77.005]}})
    print("[OK] Insertados 4 registros.")

def test_range(db: File):
    out = db.execute({
        "op":"range search",
        "field":"ubicacion",
        "point":[-12.10, -77.00],
        "r":0.02  # radio
    })
    print("\n[range_search] centro=(-12.10,-77.00), r=0.02")
    for r in out:
        print(r)

def test_knn(db: File):
    out = db.execute({
        "op":"knn",
        "field":"ubicacion",
        "point":[-12.10, -77.00],
        "k":2
    })
    print("\n[knn] k=2 cerca de (-12.10,-77.00)")
    for r in out:
        print(r)

if __name__ == "__main__":
    clean()                 
    ensure_table()          
    db = File(TAB)          
    run_inserts(db)         
    test_range(db)          
