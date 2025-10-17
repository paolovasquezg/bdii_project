# bplus_varied_inserts.py
import shutil
from backend.catalog.ddl import create_table
from backend.storage.file import File

def main():
    table = "bp_varied"
    fields = [
        {"name": "id", "type": "i", "key": "primary"},            # indexes stored in main file
        {"name": "name", "type": "s", "length": 16, "index": "bplus"},  # b+ index on name (secondary)
        {"name": "value", "type": "i"}
    ]

    # create table + files
    create_table(table, fields)
    f = File(table)

    inserts = [
        {"id": 1, "name": "alpha", "value": 10},                           # normal
        {"id": 1, "name": "alpha-dup", "value": 11},                       # duplicate indexes -> should be rejected
        {"id": 2, "name": "alpha", "value": 12},                           # duplicate name (allowed unless name marked unique)
        {"id": 3, "name": "beta", "value": 20},
        {"id": 4, "name": "gamma", "value": 30},
        {"id": 5, "name": "delta", "value": 40},
        {"id": 6, "name": "epsilon", "value": 50},
        {"id": 7, "name": "zeta", "value": 60},
        {"id": 8, "name": "theta", "value": 200},
        {"id": 9, "name": "iota", "value": 200},
        {"id": 10, "name": "this_name_is_way_too_long", "value": 999},
        {"id": 11, "name": "n11", "value": 11},
        {"id": 12, "name": "n12", "value": 12},
        {"id": 13, "name": "n13", "value": 13},
        {"id": 14, "name": "n14", "value": 14},
        {"id": 15, "name": "n15", "value": 15},
        {"id": 16, "name": "n16", "value": 16},
        {"id": 17, "name": "n17", "value": 17},
        {"id": 18, "name": "n18", "value": 18},
        {"id": 19, "name": "n19", "value": 19},
        {"id": 20, "name": "n20", "value": 20},
        {"id": 21, "name": "n21", "value": 21},
        {"id": 22, "name": "n22", "value": 22},
        {"id": 23, "name": "n23", "value": 23},
        {"id": 24, "name": "n24", "value": 24},
        {"id": 25, "name": "n25", "value": 25},
        {"id": 26, "name": "n26", "value": 26},
        {"id": 27, "name": "n27", "value": 27},
        {"id": 28, "name": "n28", "value": 28},
        {"id": 29, "name": "n29", "value": 29},
        {"id": 30, "name": "n30", "value": 30},
        {"id": 31, "name": "n31", "value": 31},
        {"id": 32, "name": "n32", "value": 32},
        {"id": 33, "name": "n33", "value": 33},
        {"id": 34, "name": "n34", "value": 34},
        {"id": 35, "name": "n35", "value": 35},
        {"id": 36, "name": "n36", "value": 36},
        {"id": 37, "name": "n37", "value": 37},
        {"id": 38, "name": "n38", "value": 38},
        {"id": 39, "name": "n39", "value": 39},
        {"id": 40, "name": "n40", "value": 40},
    ]

    print("Inserting records...")
    for rec in inserts:
        f.execute({"op": "insert", "record": rec})

    print("Search by indexed name 'alpha' ->", f.execute({"op": "search", "field": "name", "value": "alpha"}))
    print("Search by non-indexed value 200 ->", f.execute({"op": "search", "field": "value", "value": 200}))
    print("Range search ids 2-7 ->", f.execute({"op": "range_search", "field": "id", "min": 2, "max": 7}))

    print("Removing id=3 ->", f.execute({"op": "remove", "field": "id", "value": 3}))
    print("Search id=3 after delete ->", f.execute({"op": "search", "field": "id", "value": 3}))

    print("Re-inserting id=3 (beta-new)...")
    f.execute({"op": "insert", "record": {"id": 3, "name": "beta-new", "value": 777}})
    print("Search id=3 after re-insert ->", f.execute({"op": "search", "field": "id", "value": 3}))

    final = f.execute({"op": "range_search", "field": "id", "min": 20, "max": 35})
    print("Final ids:", [r["id"] for r in final])

    shutil.rmtree("files", ignore_errors=True)

if __name__ == '__main__':
    main()