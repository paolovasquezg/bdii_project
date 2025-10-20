import json

from backend.engine.engine import Engine

ENGINE = Engine()

def run_sql(sql: str) -> dict:
    env = ENGINE.run(sql)
    print(json.dumps(env, indent=2, ensure_ascii=False))
    return env

run_sql("""drop table products""")

path = "escribir path"
run_sql(f"create table products from file '{path}' using index heap(product_id)")
#run_sql("""
#CREATE TABLE products (
#  product_id INT PRIMARY KEY USING sequential,
#  name VARCHAR(32),
#  price FLOAT INDEX USING b+,
#  stock INT,
#  INDEX(name) USING hash
#);
#""")


run_sql("INSERT INTO products (product_id, name, price, stock) VALUES (6, 'mouse', 50, 1)")
run_sql("INSERT INTO products (product_id, name, price, stock) VALUES (7, 'laptop', 60, 2)")

#for i in range(1, 61):
#    run_sql(f"INSERT INTO products (product_id, name, price, stock) VALUES ({i}, 'mouse', 50, {i})")

#run_sql("CREATE INDEX IF NOT EXISTS ON products (price) USING b+")

#run_sql("SELECT * FROM products WHERE stock = 2;")

#run_sql("SELECT * FROM products WHERE name = 'laptop';")

run_sql("SELECT * FROM products;")

#run_sql("SELECT * FROM products WHERE product_id = 2")

#run_sql("SELECT * FROM products WHERE product_id BETWEEN 5 AND 7")

#run_sql("DELETE from products where product_id = 2")

#run_sql("""drop table products""")
