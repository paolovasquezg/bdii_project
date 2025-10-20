
### 🔍 **Complejidad de algoritmos (en accesos a memoria secundaria)**

#### 📌 **Parámetros**
- **n:** número total de registros almacenados en el árbol.  
- **M:** número máximo de entradas (hijos) por nodo, definido por el tamaño de página en disco.  
- Cada nodo leído o escrito implica una **operación de E/S en disco**, y por eso medimos la complejidad en número de páginas accedidas.

---

### a. **Inserción (`insert`)**

Se selecciona la hoja cuya expansión de MBR sea mínima. Si el nodo se llena, se realiza un **split cuadrático** y se actualizan los MBR hacia la raíz.

- **Promedio:** `O(log_M n)`  
- **Peor caso:** `O(n/M)` — si se producen múltiples splits y deben recorrerse o ajustar muchos nodos.

---

### b. **Búsqueda puntual (`search`)**

Se recorren únicamente los nodos cuyos MBR contienen el punto consultado.

- **Promedio:** `O(log_M n)`  
- **Peor caso:** `O(n/M)` — si los MBR se solapan y es necesario explorar casi todas las ramas.

---

### c. **Eliminación (`remove`)**

Se localiza la hoja, se elimina el registro y si cae por debajo del mínimo permitido, se redistribuye o reinserta el contenido.

- **Promedio:** `O(log_M n)`  
- **Peor caso:** `O(n/M)` — si hay reinserciones masivas o recorridos por múltiples ramas.

---

### d. **Búsqueda por rango (`range_search`)**

Se exploran los nodos cuyos MBR intersectan el área consultada. En hojas se filtran los registros.

- **Promedio:** `O(log_M n + k)`  
- **Peor caso:** `O(n/M + k)` — si la región abarca todos los nodos o hay solapamiento extremo.

---

### e. **Búsqueda de vecinos más cercanos (`knn`)**

Usa una cola de prioridad para explorar nodos ordenados por distancia mínima al punto consultado.

- **Promedio:** `O(log_M n + k log k)`  
- **Peor caso:** `O(n/M + k log k)` — si no se puede podar ninguna rama y deben evaluarse casi todos los nodos.

---

### ⚖️ **Tabla comparativa de complejidad (E/S de disco)**

| Operación     | Explicación breve | Complejidad promedio | Peor caso |
|---------------|--------------------|-----------------------|-----------|
| `insert()`    | Inserta un nuevo registro, ajusta MBRs y divide nodos si hay overflow. | `O(log_M n)` | `O(n/M)` |
| `search()`    | Busca un punto exacto en el índice. | `O(log_M n)` | `O(n/M)` |
| `remove()`    | Elimina un registro y reorganiza nodos si están subocupados. | `O(log_M n)` | `O(n/M)` |
| `range_search()` | Encuentra registros dentro de un área o radio. | `O(log_M n + k)` | `O(n/M + k)` |
| `knn()`       | Devuelve los k puntos más cercanos. | `O(log_M n + k log k)` | `O(n/M + k log k)` |

---

### ✅ **Conclusión**

El **R-Tree minimiza accesos a disco** gracias a su estructura jerárquica basada en MBRs:

- En **promedio**, mantiene un comportamiento eficiente y cercano a `O(log_M n)`.  
- En el **peor caso**, cuando hay gran solapamiento o la consulta es muy amplia, se podrían explorar **todas las páginas del árbol**, es decir `O(n/M)` accesos a memoria secundaria.

---
