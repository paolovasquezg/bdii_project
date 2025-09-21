from parser import SQLRunner

r = SQLRunner()

print(r.execute("DROP TABLE IF EXISTS products"))

print(r.execute("""
CREATE TABLE products (
  product_id INT PRIMARY KEY USING heap,
  name VARCHAR(32),
  price FLOAT INDEX USING bplus,
  stock INT,
  INDEX(name) USING hash
);
"""))

print(r.execute("CREATE INDEX IF NOT EXISTS ON products (price) USING b+"))
print(r.execute("DROP INDEX ON products (price)"))

print(r.execute("INSERT INTO products (product_id, name, price, stock) VALUES (1, 'laptop', 1500, 10)"))
print(r.execute("INSERT INTO products (product_id, name, price, stock) VALUES (2, 'mouse', 45.9, 250)"))

rows = r.execute("SELECT * FROM products WHERE product_id = 2")
print(rows)

rows = r.execute("SELECT * FROM products WHERE price BETWEEN 40 AND 100")
print(rows)

# print(r.execute("INSERT INTO products (product_id, name, price, stock) VALUES (2, 'dup', 99, 5)"))
print(r.execute("CREATE INDEX IF NOT EXISTS ON products (price) USING b+"))
print(r.execute("SELECT * FROM products WHERE price BETWEEN 40 AND 100"))

print(r.execute("DELETE FROM products WHERE product_id = 2"))
print(r.execute("SELECT * FROM products WHERE product_id = 2"))

print(r.execute("DROP TABLE products"))
