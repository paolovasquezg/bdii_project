"""
SEQUENTIAL • CSV + SQL smoke test
- Crea tabla con PRIMARY KEY USING sequential
- Importa CSV (o fallback a INSERT por fila)
- Consulta igualdad y rango sobre la PK
"""
import os, sys, csv, json
HERE = os.path.abspath(os.path.dirname(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

Engine = None
try:
    from backend.engine.engine import Engine
except Exception:
    from backend.engine import Engine  # type: ignore
ENGINE = Engine()

def run_sql(sql: str) -> dict:
    env = ENGINE.run(sql)
    print(json.dumps(env, indent=2, ensure_ascii=False))
    return env

def print_section(title: str):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

def ensure_csv(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id","name","price","stock"])
        w.writerows([
            [1,"Alpha",10.5,50],
            [2,"Bravo",20.0,15],
            [3,"Charlie",15.0,30],
            [4,"Delta",25.0,5],
            [5,"Echo",30.5,0],
        ])

def import_csv_via_sql_inserts(table: str, csv_path: str):
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            vals = []
            for k in ["product_id","name","price","stock"]:
                v = row[k]
                if v is None or v == "":
                    vals.append("NULL")
                else:
                    try:
                        if "." in v or "e" in v.lower():
                            float(v); vals.append(v)
                        else:
                            int(v); vals.append(v)
                    except Exception:
                        vals.append("'" + v.replace("'", "''") + "'")
            run_sql(f"INSERT INTO {table} VALUES ({', '.join(vals)});")

def main():
    tbl = "seq_products_sql"
    csv_path = os.path.join(HERE, "_testdata", "csv", "products.csv")
    ensure_csv(csv_path)

    print_section("SEQUENTIAL: create table + import csv")
    run_sql(f"DROP TABLE IF EXISTS {tbl};")
    run_sql(f"""
        CREATE TABLE {tbl} (
            product_id INT PRIMARY KEY USING sequential,
            name VARCHAR(32),
            price FLOAT,
            stock INT
        );
    """)
    clean = csv_path.replace('\\', '/')
    imp = run_sql(f"CREATE TABLE {tbl} FROM FILE '{clean}';")
    if not imp.get("ok", False):
        import_csv_via_sql_inserts(tbl, csv_path)

    print_section("SEQUENTIAL: consultas en PK")
    run_sql(f"SELECT * FROM {tbl} WHERE product_id = 2;")
    run_sql(f"SELECT * FROM {tbl} WHERE product_id BETWEEN 2 AND 5;")
    print("\n✅ SEQUENTIAL test completed.")

if __name__ == "__main__":
    main()
