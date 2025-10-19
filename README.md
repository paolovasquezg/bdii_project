### ⚙️ Implementación en el proyecto (R-Tree)

El índice **R-Tree** está implementado en los siguientes archivos del proyecto:

```
indexes/rtree/
│── RTree.py
│── node.py
│── mbr.py
│── metrics.py
```

Cada archivo cumple una función específica dentro de la estructura del árbol:

| Archivo | Descripción |
|----------|-------------|
| **RTree.py** | Contiene la clase principal `RTree`, responsable de las operaciones de **inserción (`insert`)**, **búsqueda puntual (`search`)**, **búsqueda por rango (`range_search`)**, **búsqueda de vecinos (`knn`)** y **eliminación (`remove`)**. Coordina la raíz del árbol y las divisiones de nodos. |
| **node.py** | Implementa la clase `Node`, que representa los nodos del árbol (internos y hojas). Gestiona las entradas y controla la recursión durante las búsquedas e inserciones. |
| **mbr.py** | Define la clase `MBR` (*Minimum Bounding Rectangle*), con operaciones geométricas como intersección, expansión y cálculo de área. Es fundamental para todas las búsquedas espaciales. |
| **metrics.py** | Incluye funciones de distancia y métricas espaciales, como la **distancia euclidiana**, utilizadas en la búsqueda de los *k* vecinos más cercanos. |

---

### 🔍 Algoritmos principales implementados y complejidad (E/S en memoria secundaria)

#### Definiciones
- **n:** número total de registros almacenados en el índice.  
- **M:** número máximo de entradas (hijos) por nodo, determinado por el tamaño del bloque en disco.  
  Cada acceso a un nodo implica una **lectura o escritura en memoria secundaria**, por lo que las complejidades reflejan **número estimado de accesos a páginas de disco**.

---

#### a. Inserción (`insert`)
1. Se calcula el **MBR** del nuevo elemento.  
2. Se selecciona el nodo hoja que requiere **menor aumento de área** (*mínimo enlargement*).  
3. Si el nodo se desborda (supera M entradas), se ejecuta un **split cuadrático**.  
4. Se actualizan los MBR ascendentes hasta la raíz.

**Complejidad de E/S:**  
- Promedio: `O(log_M n)`  
- Peor caso: `O(M²)` (por divisiones sucesivas y reescritura de bloques).

**Representación gráfica del proceso de inserción:**

![Diagrama de inserción en el R-Tree](images/RtreeInsert.png)

---

#### b. Búsqueda puntual (`search`)
1. Se calcula el MBR asociado al punto de búsqueda.  
2. Se leen recursivamente los nodos cuyos MBR contengan el punto.  
3. Se devuelve el registro si se encuentra en un nodo hoja.

**Complejidad de E/S:**  
- Promedio: `O(log_M n)`  
- Peor caso: `O(n)` (si existe solapamiento total entre nodos).

---

#### c. Eliminación (`remove`)
1. Se localiza el nodo hoja que contiene el registro.  
2. Se elimina la entrada correspondiente.  
3. Si el nodo queda por debajo del factor mínimo de llenado, se redistribuyen o fusionan nodos y se actualizan los MBR ascendentes.

**Complejidad de E/S:**  
- Promedio: `O(log_M n)`  
- Peor caso: `O(n)` (si se requiere reorganizar varios niveles del árbol).

**Representación gráfica del proceso de eliminación:**

![Diagrama de eliminación en el R-Tree](images/RtreeRemove.png)

---

#### d. Búsqueda por rango (`range_search`)
1. Se construye un MBR de consulta a partir del punto central y un radio o región rectangular.  
2. Se recorren los nodos cuyos MBR **intersecten** el MBR de consulta.  
3. En los nodos hoja, se devuelven los registros dentro del rango.

**Complejidad de E/S:**  
- Promedio: `O(log_M n + k)`  
- Peor caso: `O(n)` (cuando los MBR se solapan ampliamente).

---

#### e. Búsqueda de vecinos cercanos (`knn`)
1. Se calcula la distancia del punto de consulta a los MBR de cada nodo.  
2. Se usa una **cola de prioridad (min-heap)** para expandir primero los nodos más cercanos.  
3. Se devuelven los **k registros más próximos**.

**Complejidad de E/S:**  
- Promedio: `O(log_M n + k log k)`  
- Peor caso: `O(n log k)` (si se deben recorrer todos los nodos).

---

### ⚖️ Tabla comparativa de métodos y complejidades (E/S de disco)

| Operación | Descripción | Complejidad promedio (E/S) | Peor caso (E/S) |
|------------|--------------|-----------------------------|-----------------|
| **insert()** | Inserta un registro, recalculando MBRs y dividiendo nodos si es necesario. | O(log_M n) | O(M²) |
| **search()** | Busca un registro puntual en el espacio. | O(log_M n) | O(n) |
| **remove()** | Elimina un registro y reajusta los nodos afectados. | O(log_M n) | O(n) |
| **range_search()** | Devuelve todos los registros dentro de una región espacial o radio. | O(log_M n + k) | O(n) |
| **knn()** | Devuelve los *k* registros más cercanos al punto consultado. | O(log_M n + k log k) | O(n log k) |

---

En conclusión, el **R-Tree** optimiza las operaciones espaciales reduciendo el número de accesos a disco mediante una organización jerárquica basada en MBRs.  
Su diseño permite búsquedas eficientes en dominios multidimensionales, haciendo que las operaciones de inserción, eliminación y consulta mantengan un costo logarítmico promedio respecto al número total de registros.
