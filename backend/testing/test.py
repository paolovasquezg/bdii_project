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


for i in range(1, 61):
    run_sql(f"INSERT INTO products (product_id, name, price, stock) VALUES ({i}, 'mouse', 50, {i})")

#run_sql("CREATE INDEX IF NOT EXISTS ON products (price) USING b+")

#run_sql("SELECT * FROM products WHERE price = 50;")

run_sql("SELECT * FROM products WHERE name = 'mouse';")

#run_sql("SELECT * FROM products WHERE product_id = 2")

#run_sql("SELECT * FROM products WHERE product_id BETWEEN 1 AND 60")

#run_sql("DELETE from products where product_id = 2")

run_sql("""drop table products""")
