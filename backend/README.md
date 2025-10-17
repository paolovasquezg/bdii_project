# BDII – Backend

Motor simplificado de base de datos para el curso BD2: **parser → planner → executor → storage**, con motores de almacenamiento **heap**, **sequential** y **ISAM** (R-Tree en progreso). Incluye API HTTP para consultas tipo SQL y una batería de tests de humo.

## Tabla de contenidos

* [Arquitectura](#arquitectura)
* [Estructura de carpetas](#estructura-de-carpetas)
* [Instalación y requisitos](#instalación-y-requisitos)
* [Cómo ejecutar](#cómo-ejecutar)
* [API HTTP](#api-http)
* [Formato de respuesta (URE)](#formato-de-respuesta-ure)
* [Soporte SQL (subset)](#soporte-sql-subset)
* [Motores de almacenamiento](#motores-de-almacenamiento)
* [Importación desde CSV](#importación-desde-csv)
* [Tests](#tests)
* [Convenciones y decisiones clave](#convenciones-y-decisiones-clave)
* [Solución de problemas](#solución-de-problemas)
* [Hoja de ruta / TODO](#hoja-de-ruta--todo)
* [Licencia](#licencia)

---

## Arquitectura

**Flujo principal**

1. **Parser**: tokeniza y valida la sentencia SQL (subset), y produce un AST/plan preliminar.
2. **Planner**: normaliza/expande opciones (e.g., `USING isam`) y genera un plan ejecutable.
3. **Executor**: ejecuta el plan y retorna resultados en formato **URE**.

   * Clasifica `kind` como `ddl` o `dml`.
   * En `INSERT` completa `meta.affected`.
   * En `CREATE TABLE ... FROM FILE` usa *bulk build* para ISAM.
4. **Storage**:

   * **File** (dispatcher) elige el primario: `HeapFile`, `SeqFile` o `IsamFile`.
   * Lectura/escritura de páginas y registros (`core/page`, `core/record`).
   * Catálogo y metadatos (`catalog`).

---

## Estructura de carpetas

```
backend/
├─ main.py                 # API FastAPI (uvicorn backend.main:app)
├─ engine/
│  ├─ engine.py            # Orquestador Engine.run(sql)
│  ├─ parser.py            # Parser SQL (subset)
│  ├─ planner.py           # Planner
│  └─ executor.py          # Executor (URE)
├─ storage/
│  ├─ file.py              # Dispatcher primario (heap/seq/isam)
│  ├─ primary/
│  │  ├─ heap.py           # HeapFile
│  │  ├─ sequential.py     # SeqFile
│  │  └─ isam.py           # IsamFile
│  └─ secondary/
│     └─ rtree.py          # (en integración)
├─ core/
│  ├─ record.py            # Representación de registros
│  ├─ page.py              # Estructura de páginas
│  └─ utils.py             # build_format, helpers, etc.
├─ catalog/
│  ├─ catalog.py           # get_json, put_json, paths
│  └─ ddl.py               # CREATE/DROP table/index, load/save
├─ runtime/
│  └─ files/
│     └─ <tabla>/
│        ├─ <tabla>.dat        # metadatos (JSON header) + esquema físico
│        ├─ <tabla>_data.dat   # datos
│        ├─ <tabla>_delete.dat # mapa de borrados
│        └─ <tabla>_index.dat  # índice (ISAM)
└─ testing/
   ├─ parser_test.py
   ├─ heap_test.py
   ├─ seq_test.py
   ├─ isam_test.py
   └─ rtree_test.py
```

> Asegúrate de que **todas** las carpetas con módulos tengan `__init__.py`.

---

## Instalación y requisitos

* **Python** 3.10+ recomendado
* Dependencias mínimas para la API:

  * `fastapi`
  * `uvicorn`
  * `pydantic`

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install fastapi uvicorn pydantic
```

*(Si tienes `requirements.txt`, usa `pip install -r requirements.txt`.)*

---

## Cómo ejecutar

### API

Ejecuta uvicorn **desde la raíz del proyecto**:

```bash
uvicorn backend.main:app --reload --port 8080
```

> **Importante:** Ejecutar desde la raíz evita `ModuleNotFoundError: No module named 'backend'`.

### CLI/Script

También puedes invocar el motor desde scripts de prueba (ver carpeta `testing/`).

### Interfaz tester

Ejecutar **desde la raíz del proyecto**.


```bash
python -m http.server 5500
```

---

## API HTTP

### `POST /query`

Recibe `{ "sql": "<sentencias separadas por ';'>" }` y retorna **URE** (ver abajo).

**Ejemplo:**

```bash
curl -s http://127.0.0.1:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"DROP TABLE IF EXISTS products; CREATE TABLE products(id INT PRIMARY KEY USING heap, name VARCHAR(32)); INSERT INTO products (id,name) VALUES (1, \"dup\"); SELECT * FROM products WHERE id=1;"}' | jq .
```

### `GET /health`

Healthcheck simple.

---

## Formato de respuesta (URE)

Envoltorio **Uniform Result Envelope**:

```json
{
  "ok": true,
  "schema": "bd2.v1",
  "results": [
    {
      "ok": true,
      "kind": "ddl | dml | error | query",
      "action": "create_table | drop_table | create_index | insert | search | range_search | knn | remove | ...",
      "count": 1,
      "data": [ { "id": 1, "name": "dup", "price": 99.0 } ],
      "meta": { "affected": 1 },
      "table": "products",
      "error": null
    }
  ],
  "warnings": [],
  "stats": { "time_ms": 3.21 }
}
```

**Convenciones:**

* DDL → `kind: "ddl"`.
* DML (incluye SELECT/SEARCH/RANGE/KNN) → `kind: "dml"`.
* `INSERT` debe devolver `meta.affected >= 1`.
* `count = len(data)` para lecturas.

En caso de error, `ok:false` y un único `result` con `kind:"error"` y `error.code/message`.

---

## Soporte SQL (subset)

* `DROP TABLE [IF EXISTS] <tabla>`
* `CREATE TABLE <tabla> (cols...) [USING <heap|sequential|isam>]`

  * Columnas: `INT`, `FLOAT/REAL/DOUBLE`, `VARCHAR(n)`, `BOOL`
  * `PRIMARY KEY ... USING <heap|sequential|isam>`
* `CREATE INDEX <idx> ON <tabla>(col) USING <...>` *(secundarios en progreso)*
* `INSERT INTO <tabla> (cols...) VALUES (...);`
* `SELECT * FROM <tabla> WHERE <pk> = v;`
* `SELECT * FROM <tabla> WHERE <pk> BETWEEN a AND b;`
* `DELETE FROM <tabla> WHERE <pk> = v;`
* `CREATE TABLE <tabla> FROM FILE '<ruta.csv>'` (ver Importación)

---

## Motores de almacenamiento

### Dispatcher primario

`storage/file.py` selecciona el motor según el catálogo:

```python
def _prim_file_cls(self):
    kind = (self.indexes or {}).get("indexes", {}).get("index", "heap").lower()
    return {"heap": HeapFile, "sequential": SeqFile, "seq": SeqFile, "isam": IsamFile}.get(kind, HeapFile)
```

### Header unificado

Todos los primarios comparten **esquema físico** escrito en el datafile principal, **incluyendo** flag lógico `deleted` para un `build_format` consistente.

### ISAM – notas clave

* Necesita **inicialización** de páginas **antes** del primer insert masivo.
* El **executor** usa *bulk build* cuando el primario es `isam` en `CREATE TABLE ... FROM FILE`.
* Formato de `struct` para claves: mapear tipos a `i`, `f`, `Ns`, `?` (evitar `"int"`, `"varchar"`, etc.).

---

## Importación desde CSV

### `CREATE TABLE ... FROM FILE '<csv>'`

* Si el primario es **ISAM**, el executor ejecuta `File.execute({"op":"build", "records":[...]})` para:

  1. Inicializar raíz/hojas/páginas.
  2. Insertar registros en orden correcto.
* Si el primario es **heap** o **sequential**, usa `import_csv` (fila a fila).

---

## Tests

En `backend/testing/` hay pruebas de humo:

* `parser_test.py` – contrato URE y flujo base.
* `heap_test.py` – operaciones en heap.
* `seq_test.py` – sequential.
* `isam_test.py` – ISAM (igualdad y rango).
* `rtree_test.py` – en progreso.

**Runner opcional** (si lo incluyes en el repo):

```bash
python run_all_tests.py
# o solo algunos:
python run_all_tests.py --only parser,isam
```

Cada test debe terminar con `exit code 0` en caso de PASS.

---

## Convenciones y decisiones clave

* **`kind`**:

  * `ddl` → `create_table`, `drop_table`, `create_index`, `drop_index`, `create_table_from_file`.
  * `dml` → `insert`, `remove`, `search`, `range_search`, `knn`, etc.
* **`meta.affected`**:

  * `insert` y `remove` deben informar filas afectadas (`>=1`).
* **Header del datafile (todos)**:

  * Siempre se escribe el header con el esquema físico y `deleted`.
* **Select/Query**:

  * `count = len(data)`, sin exponer campos internos (`deleted`, `pos`, etc.).

---

## Solución de problemas

### `ModuleNotFoundError: No module named 'backend'`

* Lanza uvicorn **desde la raíz**:

  ```bash
  uvicorn backend.main:app --reload
  ```
* Asegura `__init__.py` en paquetes.

### `bad char in struct format`

* En ISAM, mapea tipos a códigos válidos de `struct` (`i`, `f`, `Ns`, `?`).

### `unpack requires a buffer of 4 bytes` / lecturas inconsistentes

* Falta **header** en el datafile (esquema físico).
  Solución: DDL debe **siempre** escribir header para cualquier primario.

### `INSERT` se ve como afectadas 0

* Asegura que el executor complete `meta.affected` (o que el storage lo retorne).

---

## Hoja de ruta / TODO

* [ ] Índices secundarios (Hash, B+-Tree, GiST/R-Tree estable).
* [ ] `CREATE INDEX` secundario end-to-end (planner + executor + storage/secondary).
* [ ] Optimización de scans (predicados, proyección).
* [ ] Métricas/telemetría por operación y páginas leídas/escritas.
* [ ] CI simple (GitHub Actions) corriendo los tests de humo.

---

## Licencia

Uso académico. Ajusta según políticas de tu curso.
