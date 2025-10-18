import json

from backend.engine.engine import Engine

ENGINE = Engine()

def run_sql(sql: str) -> dict:
    env = ENGINE.run(sql)
    print(json.dumps(env, indent=2, ensure_ascii=False))
    return env

run_sql("drop table pk_switch_rtree")

run_sql("""
CREATE TABLE pk_switch_rtree (
  id INT PRIMARY KEY USING bplus,
  name VARCHAR(16),
  coords VARCHAR(64) INDEX USING rtree
);
""")

# -- datos base (densidad cerca de (55,3) + outliers) --
rows = [
    (1,  "p1",   "55,3"),  # centro
    (2,  "p2",   "54,3"),
    (3,  "p3",   "56,4"),
    (4,  "p4",   "53,2"),
    (5,  "p5",   "58,3"),
    (6,  "p6",   "52,2"),
    (7,  "p7",   "59,4"),
    (8,  "p8",   "57,5"),
    (9,  "p9",   "55,6"),
    (10, "p10",  "60,2"),
    # algunos más cerca
    (11, "p11",  "55,4"),
    (12, "p12",  "54,2"),
    (13, "p13",  "56,3"),
    (14, "p14",  "53,3"),
    (15, "p15",  "57,3"),
    # outliers
    (16, "o1",  "200,200"),
    (17, "o2",  "0,0"),
    (18, "o3",  "120,80"),
    (19, "o4",  "350,1"),
    (20, "o5",  "10,90"),
]

for pid, name, xy in rows:
    run_sql(f"INSERT INTO pk_switch_rtree (id, name, coords) VALUES ({pid}, '{name}', '{xy}')")

run_sql(f"INSERT INTO pk_switch_rtree VALUES (21, 'o6', '50,4'),(22, 'o7', '21,3')")


run_sql(
    '''
    SELECT id, name, coords
FROM pk_switch_rtree
WHERE coords KNN (POINT(55, 3), 3);

    '''
)
