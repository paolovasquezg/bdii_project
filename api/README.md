
# Proyecto BD II

## Estructura del proyecto

```
bdii_project/
├─ api/
│  └─ main.py                # FastAPI: /query -> Engine.run(sql)
├─ catalog/
│  ├─ catalog.py             # rutas, get/put_json, tables.dat
│  ├─ ddl.py                 # create_table / create_index / drop_*
│  └─ settings.py            # DATA_DIR (por defecto runtime/files)
├─ core/
│  ├─ record.py              # helpers de registro (si se usa)
│  └─ types.py               # normalización de tipos binarios, etc.
├─ engine/
│  ├─ engine.py              # orquestador: parse -> plan -> execute
│  └─ executor.py            # ejecuta planes (invoca catalog + storage)
├─ planner/
│  └─ planner.py             # AST -> Plan (sin I/O)
├─ sql/
│  └─ parser.py              # parser puro (SQL -> AST)
├─ storage/
│  ├─ file.py                # fachada de acceso a datos
│  └─ primary/
│     ├─ heap.py
│     ├─ sequential.py
│     └─ isam.py
│  └─ secondary/
│     ├─ hash.py
│     ├─ bplus.py
│     └─ rtree.py
├─ runtime/
│  └─ files/                 # datos en tiempo de ejecución (DATA_DIR)
│     ├─ tables.dat          # catálogo global (pickle)
│     └─ <tabla>/
│        ├─ <tabla>.dat      # metadato JSON [relation, indexes]
│        └─ <tabla>-<index>-<pk/col>.dat
├─ requirements.txt
└─ README.md                 # (este archivo)
```

## Cómo correrlo

### 1) Requisitos

* Python 3.10+ recomendado
* Instalar dependencias:

```bash
pip install -r requirements.txt
```

### 2) (Opcional) Configurar directorio de datos

Por defecto los datos van a `runtime/files`. 

```bash
export BD2_DATA_DIR=/ruta/a/mis_datos
```

> `catalog/settings.py` lee `BD2_DATA_DIR` y crea el directorio si no existe.

### 3) Levantar la API

```bash
uvicorn api.main:app --reload
```

* `GET /` → ping
* `POST /query` con body `{"content": "<SQL>"}` → ejecuta el SQL y devuelve resultados por sentencia.

### 4) Probar rápido

```python
from api.engine.engine import Engine

e = Engine()

print(e.run("""
CREATE TABLE products (
  product_id INT PRIMARY KEY USING heap,
  name VARCHAR(32),
  price FLOAT INDEX USING bplus,
  stock INT,
  INDEX(name) USING hash
);"""))

print(e.run("CREATE INDEX IF NOT EXISTS ON products (price) USING b+;"))
print(e.run("INSERT INTO products (product_id, name, price, stock) VALUES (2, 'dup', 99, 5);"))
print(e.run("SELECT * FROM products WHERE price BETWEEN 40 AND 100 AND name = 'dup';"))
print(e.run("DELETE FROM products WHERE product_id = 2;"))
```

## ¿Qué hace cada capa?

### `sql/parser.py` (parser puro)

Convierte texto SQL a **ASTs** (dataclasses/dicts). No toca disco ni valida semántica.
Soporta:

* `CREATE TABLE` con `PRIMARY KEY USING ...`, `INDEX USING ...` e índice de tabla (`INDEX(col) USING ...`)
* `CREATE INDEX [IF NOT EXISTS] [name] ON t(col) USING ...`
* `DROP TABLE [IF EXISTS]`, `DROP INDEX ...`
* `INSERT INTO t (cols) VALUES (...)`
* `SELECT [*|cols] FROM t [WHERE ...]` con `=, !=, <, <=, >, >=, BETWEEN, AND, OR`
* `DELETE FROM t [WHERE ...]` (igualdad)

> El parser **solo** verifica estructura; la validación de tabla/columna existe se delega a la ejecución.

### `planner/planner.py` (planificador)

Traduce AST → **planes de ejecución** (dicts simples). No hace I/O.
Ejemplos:

* `price BETWEEN a AND b` → `{"action":"range search","table":...,"field":"price","min":a,"max":b}`
* `name = 'x'` → `{"action":"search","table":...,"field":"name","value":"x"}`
* `INSERT` → `{"action":"insert","table":...,"record":{...}}`
* `CREATE TABLE` / `CREATE INDEX` / `DROP *` → planes DDL
* Caso común `BETWEEN AND =` se resuelve como **range search** con `post_filter` para la igualdad adicional.

### `engine/engine.py` y `engine/executor.py`

* `Engine.run(sql)`:

  1. parsea (`parser`),
  2. planifica (`planner`),
  3. ejecuta (`executor`).
* `Executor.run(plans)`:

  * **DDL**: llama a `catalog/ddl.py` (`create_table`, `create_index`, `drop_*`).
  * **DML**: llama a `storage/file.py` con `{op: "insert"|"search"|"range search"|"remove"}`.
  * Aplica `post_filter` cuando el plan lo trae (p.ej., `AND name = 'dup'`).

### `catalog` (metadatos y DDL)

* `settings.py`: define `DATA_DIR`.
* `catalog.py`: funciones utilitarias:

  * `table_dir(name)`, `table_meta_path(name)`
  * `get_json(path, default)`, `put_json(path, obj)`
  * `load_tables()`, `save_tables()` → `runtime/files/tables.dat`
* `ddl.py`: crea/borra estructuras:

  * `create_table(name, fields)` → escribe el metadato `<tabla>.dat` con:

    ```json
    [
      { "col": {"type": "...", "length": ..., "key": "primary" } , ... },   // relation
      { "primary": {"index": "heap", "filename": "<path>"}, "price": {"index":"bplus",...}, ... } // indexes
    ]
    ```

    además genera archivos para el **primario** y los **secundarios** (vacíos).
  * `create_index(table, col, method)`, `drop_table`, `drop_index`.

### `storage` (acceso físico)

* `file.py` es la **fachada**: usa el metadato para decidir a qué implementación delegar (heap, sequential, isam).
* `primary/heap.py`, `primary/sequential.py`, `primary/isam.py`, `secondary/hash.py`, `secondary/bplus.py`, `secondary/rtree.py`: operaciones sobre archivos de datos (insert/search/range/remove según soporte).

> **Índices secundarios (hash / b+)**: están pendientes de implementar. Por ahora, cuando un plan requiere un secundario, `file.py` **hace fallback** al primario (normalmente heap) para no dejar de responder.

## Formato de archivos

* **`runtime/files/tables.dat`**: diccionario `{tabla: ruta_a_<tabla>.dat}` (pickle).
* **`runtime/files/<tabla>/<tabla>.dat`**: JSON con `[relation, indexes]`.
* **Primario**: JSON con `[[schema_de_campos] + {"deleted": "?"}]`.
* **Secundarios**: JSON con `[[campo_indexado], {"name":"pk",...}, {"name":"deleted","type":"?"}]`.

> El campo `"deleted"` es lógico. Las búsquedas filtran registros `deleted == True`.

## Qué SQL está soportado (y límites actuales)

* ✅ `CREATE TABLE` con PK e índices (tabla y por columna).
* ✅ `CREATE INDEX`, `DROP TABLE`, `DROP INDEX`.
* ✅ `INSERT` de una fila por sentencia.
* ✅ `SELECT ... WHERE` con:

  * Igualdad (`=`) y diferencias (`!=`, `<>`)
  * Comparadores (`<, <=, >, >=`)
  * `BETWEEN`
  * `AND`/`OR` simples (optimizados: `BETWEEN AND =`).
* ✅ `DELETE ... WHERE` (igualdad simple).

**Por hacer / ideas:**

* Multi-VALUES en `INSERT`.
* `UPDATE`.
* Índices secundarios reales (hash / B+):

  * `storage/secondary/hash_index.py`
  * `storage/secondary/bplus.py`
  * Integración en `storage/file.py` en las ramas `indx in ("hash","b+")`.

## Ejemplos de uso (API)

```bash
# Crear tabla
curl -s -X POST localhost:8000/query -H "Content-Type: application/json" \
  -d '{"content":"CREATE TABLE products ( product_id INT PRIMARY KEY USING heap, name VARCHAR(32), price FLOAT INDEX USING bplus, stock INT, INDEX(name) USING hash );"}'

# Insertar
curl -s -X POST localhost:8000/query -H "Content-Type: application/json" \
  -d '{"content":"INSERT INTO products (product_id, name, price, stock) VALUES (2, ''dup'', 99, 5);"}'

# Consultar
curl -s -X POST localhost:8000/query -H "Content-Type: application/json" \
  -d '{"content":"SELECT * FROM products WHERE price BETWEEN 40 AND 100 AND name = ''dup'';"}'
```

## Troubleshooting

* **`ModuleNotFoundError: No module named 'settings'`**
  Asegúrate de tener `catalog/__init__.py` y que los imports en `catalog.py` usen:

  ```python
  try:
      from .settings import DATA_DIR
  except ImportError:
      from settings import DATA_DIR
  ```

* **`FileNotFoundError ... runtime/files/.../<tabla>.dat`**
  Hay desalineación entre dónde se crea la tabla y `DATA_DIR`.

  * O recrea la tabla después de fijar `BD2_DATA_DIR`
  * O mueve manualmente `files/<tabla>` a `runtime/files/<tabla>`.

* **`SELECT` devuelve `[]` pero insertaste**
  Si el predicado usa un índice secundario (hash/b+), y aún no está implementado, `file.py` cae al primario. Revisa que:

  * el primario sea `heap` (o que tu `sequential/isam` ya tengan `search`/`range` implementado),
  * no estés filtrando por `"deleted": True`.

## Extender: implementar hash y B+

1. Crea `storage/secondary/hash_index.py` y/o `bplus.py` con API mínima:

   ```python
   class HashIndex:
       def __init__(self, filename, schema): ...
       def insert(self, key, pos): ...
       def search(self, key)->list[int]: ...
       def delete(self, key)->list[int]: ...
   ```

2. En `storage/file.py`, en la rama:

   ```python
   indx = self.indexes[field]["index"]
   if indx == "hash":
       # usar HashIndex(filename).search(value) -> posiciones
       # si buscas registros completos: mapear pos -> HeapFile.search_by_pos
   elif indx == "b+":
       # similar con B+ y rangos (min/max)
   ```

3. Mantener el **fallback** al primario para evitar romper la API mientras desarrollas.
