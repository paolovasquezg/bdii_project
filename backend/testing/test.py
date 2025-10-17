import json

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

run_sql("""drop table products""")


run_sql("""
CREATE TABLE products (
  product_id INT PRIMARY KEY USING sequential,
  name VARCHAR(32),
  price FLOAT INDEX USING b+,
  stock INT,
  INDEX(name) USING hash
);
""")


run_sql("INSERT INTO products (product_id, name, price, stock) VALUES (1, 'laptop', 1500, 10)")
run_sql("INSERT INTO products (product_id, name, price, stock) VALUES (1, 'laptop', 1500, 10)")

run_sql("INSERT INTO products (product_id, name, price, stock) VALUES (2, 'mouse', 45.9, 250)")

#run_sql("CREATE INDEX IF NOT EXISTS ON products (price) USING b+")

run_sql("SELECT * FROM products WHERE name = 'laptop';")

#run_sql("SELECT * FROM products WHERE product_id = 2")

#run_sql("SELECT * FROM products WHERE price BETWEEN 40 AND 100")

#run_sql("DELETE from products where product_id = 2")

run_sql("""drop table products""")
